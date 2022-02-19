use crate::sql::ast::Query;
use crate::sql::ast::SelectQuery;
use crate::sql::ast::ShowFieldKeysQuery;
use crate::sql::ast::ShowMeasurementsQuery;
use crate::sql::ast::ShowTagKeysQuery;
use crate::sql::ast::ShowTagValuesQuery;
use crate::sql::sqlparser::QueryParser;
use crate::storage::MetricsData;
use crate::storage::ProtectedStorageReader;
use anyhow::{Context, Error};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$(($k, $v),)*]))
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$($v,)*]))
    }};
}

#[derive(Deserialize, Serialize, Debug)]
pub struct QueryResult {
    pub results: Vec<StatementSeries>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StatementSeries {
    pub statement_id: String,
    pub series: Vec<Series>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Series {
    pub name: String,
    pub tags: HashMap<String, String>, // todo: change to vector of tuples?
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>, // todo: change from string to valuetype
}

pub struct QueryEngine {
    query_parser: QueryParser,
    storage_snapshot: Arc<RwLock<MetricsData>>,
}

impl QueryEngine {
    pub fn new(snapshot: Arc<RwLock<MetricsData>>) -> Self {
        QueryEngine {
            query_parser: QueryParser::new(),
            storage_snapshot: snapshot,
        }
    }

    fn select_query(&self, query: SelectQuery) -> Result<QueryResult, Error> {
        let mut table = HashMap::<u64, Vec<String>>::new();
        let columns = query.fields.len();
        let field_names: Vec<String> = query
            .fields
            .iter()
            .map(|f| format!("{}:{}", query.from, f.field_name))
            .collect();
        for field_idx in 0..columns {
            let metric_name = unsafe { field_names.get_unchecked(field_idx) };
            for metric in self.storage_snapshot.read_metrics(&metric_name).iter() {
                if !table.contains_key(&metric.timestamp) {
                    let mut columns = vec!["null".to_string(); columns + 1];
                    columns[0] = metric.timestamp.to_string();
                    table.insert(metric.timestamp, columns);
                }
                //todo: unwrap
                table.get_mut(&metric.timestamp).unwrap()[field_idx + 1] = metric.value.to_string();
            }
        }
        let mut tags = HashMap::new();
        for tag in query.where_constraints.iter() {
            tags.insert(tag.source.clone(), tag.value.clone());
        }

        let mut columns_def = vec!["time".to_string()];
        columns_def.extend(query.fields.iter().map(|f| f.field_name.to_string()));
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: query.from,
                    tags: tags,
                    columns: columns_def,
                    values: table.into_values().collect(),
                    // ,vec![
                    // vec!["1644150540000".into(), "609.8712651650578".into()],
                    // vec!["1644150600000".into(), "608.1835093669324".into()],
                    // vec!["1644150660000".into(), "609.9034402394105".into()],
                    // ],
                }],
            }],
        })
    }

    fn tag_keys_query(&self, query: ShowTagKeysQuery) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: "logins.count".into(),
                    tags: HashMap::default(),
                    columns: vec!["tagKey".into()],
                    values: vec![
                        vec!["datacenter".into()],
                        vec!["hostname".into()],
                        vec!["source".into()],
                    ],
                }],
            }],
        })
    }

    fn tag_values_query(&self, query: ShowTagValuesQuery) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: "logins.count".into(),
                    tags: HashMap::default(),
                    columns: vec!["key".into(), "value".into()],
                    values: vec![vec!["datacenter".into(), "america".into()]],
                }],
            }],
        })
    }

    fn field_keys_query(&self, query: ShowFieldKeysQuery) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: "logins.count".into(),
                    tags: HashMap::default(),
                    columns: vec!["fieldKey".into(), "fieldType".into()],
                    values: vec![vec!["value".into(), "float".into()]],
                }],
            }],
        })
    }

    fn measurements_query(&self, query: ShowMeasurementsQuery) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: "measurements".into(),
                    tags: HashMap::new(),
                    columns: vec!["name".into()],
                    values: vec![
                        vec!["cpu".into()],
                        vec!["logins.count".into()],
                        vec!["payment.ended".into()],
                        vec!["payment.started".into()],
                    ],
                }],
            }],
        })
    }

    pub fn run_query(&self, query: &str) -> Result<QueryResult, Error> {
        let queryr = self
            .query_parser
            .parse(query)
            .map_err(|e| e.map_token(|t| t.to_string()))
            .with_context(|| "query is not valid")?;

        match queryr {
            Query::Select(query) => self.select_query(query),
            Query::TagKeys(query) => self.tag_keys_query(query),
            Query::TagValues(query) => self.tag_values_query(query),
            Query::FieldKeys(query) => self.field_keys_query(query),
            Query::Measurements(query) => self.measurements_query(query),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::{DataPoint, StorageWriter};

    #[test]
    fn basic_select_query() {
        let snapshot = Arc::new(RwLock::new(HashMap::default()));
        let mut write = snapshot.write();
        (*write).add_bulk(&vec![
            DataPoint::new(Arc::from("table1:metric1"), 100u64, 10i64),
            DataPoint::new(Arc::from("table1:metric1"), 101u64, 12i64),
        ]);

        drop(write);

        let engine = QueryEngine::new(snapshot.clone());

        let mut result = dbg!(engine
            .run_query(
                "SELECT \"metric1\", \"metric2\" FROM \"table1\" WHERE \"host\"=\"localhost\""
            )
            .unwrap());

        assert_eq!(1, result.results.len());
        assert_eq!(1, result.results[0].series.len());
        let metrics = result
            .results
            .first_mut()
            .unwrap()
            .series
            .first_mut()
            .unwrap();
        assert_eq!(metrics.name, "table1");
        assert_eq!(metrics.columns, vec!("time", "metric1", "metric2"));
        metrics.values.sort_by_key(|v| v[0].parse::<u64>().unwrap());
        assert_eq!(
            metrics.values,
            vec![vec!["100", "10", "null"], vec!["101", "12", "null"]]
        );
        assert_eq!(
            metrics.tags,
            collection!["host".to_string() => "localhost".to_string()]
        );
    }
}

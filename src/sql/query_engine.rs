use crate::sql::ast::Query;
use crate::sql::ast::SelectQuery;
use crate::sql::ast::ShowFieldKeysQuery;
use crate::sql::ast::ShowMeasurementsQuery;
use crate::sql::ast::ShowTagKeysQuery;
use crate::sql::ast::ShowTagValuesQuery;
use crate::sql::sqlparser::QueryParser;
use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
}

impl QueryEngine {
    pub fn new() -> Self {
        QueryEngine {
            query_parser: QueryParser::new(),
        }
    }

    fn select_query(&self, query: SelectQuery) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: query.from,
                    tags: collection!["hostname".into() => "10.1.100.1".into()],
                    columns: vec!["time".into(), "mean".into()],
                    values: vec![
                        vec!["1644150540000".into(), "609.8712651650578".into()],
                        vec!["1644150600000".into(), "608.1835093669324".into()],
                        vec!["1644150660000".into(), "609.9034402394105".into()],
                    ],
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

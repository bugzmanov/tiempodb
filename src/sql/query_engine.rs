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

pub struct QueryEngine {}

#[derive(Deserialize, Serialize, Debug)]
pub struct QueryResult {
    results: Vec<StatementSeries>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StatementSeries {
    statement_id: String,
    series: Vec<Series>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Series {
    name: String,
    tags: HashMap<String, String>, // todo: change to vector of tuples?
    columns: Vec<String>,
    values: Vec<Vec<String>>, // todo: change from string to valuetype
}

impl QueryEngine {
    pub fn run_query(&self, query: &str) -> QueryResult {
        QueryResult {
            results: vec![StatementSeries {
                statement_id: "0".into(),
                series: vec![Series {
                    name: "logins.count".into(),
                    tags: collection!["hostname".into() => "10.1.100.1".into()],
                    columns: vec!["time".into(), "mean".into()],
                    values: vec![
                        vec!["1644150540000".into(), "609.8712651650578".into()],
                        vec!["1644150600000".into(), "608.1835093669324".into()],
                        vec!["1644150660000".into(), "609.9034402394105".into()],
                    ],
                }],
            }],
        }
    }
}

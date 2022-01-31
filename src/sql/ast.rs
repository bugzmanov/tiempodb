use core::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum SelectionType {
    Bottom,
    First,
    Last,
    Max,
    Min,
    Percentile(u8),
    //aggregates
    Count,
    Distinct,
    Integral,
    Mean,
    Median,
    Mod,
    Sum,

    Identity,
}

#[derive(Debug)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub struct FieldProjection {
    pub field_name: String,
    pub selection_type: SelectionType,
}

impl FieldProjection {
    pub fn simple(field_name: &str) -> Self {
        FieldProjection {
            field_name: field_name.to_string(),
            selection_type: SelectionType::Identity,
        }
    }

    pub fn max(field_name: &str) -> Self {
        FieldProjection {
            field_name: field_name.to_string(),
            selection_type: SelectionType::Max,
        }
    }

    pub fn mean(field_name: &str) -> Self {
        FieldProjection {
            field_name: field_name.to_string(),
            selection_type: SelectionType::Mean,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ComparisonType {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    NotLike,
}

#[derive(Debug, PartialEq)]
pub enum Time {
    NanoSeconds(u64),
    MicroSeconds(u64),
    MilliSeconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
    Days(u64),
}

impl Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Time::NanoSeconds(value) => write!(f, "{}ns", value),
            Time::MicroSeconds(value) => write!(f, "{}micros", value),
            Time::MilliSeconds(value) => write!(f, "{}ms", value),
            Time::Seconds(value) => write!(f, "{}s", value),
            Time::Minutes(value) => write!(f, "{}m", value),
            Time::Hours(value) => write!(f, "{}h", value),
            Time::Days(value) => write!(f, "{}d", value),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Fill {
    Linear,
    None,
    Null,
    Previous,
}

#[derive(Debug, PartialEq, Default)]
pub struct GroupBy {
    pub by_time: Option<Time>,
    pub by_field: Option<String>,
    pub fill: Fill,
}

impl Default for Fill {
    fn default() -> Self {
        Fill::None
    }
}

#[derive(Debug, PartialEq)]
pub struct Condition {
    pub source: String,
    pub comparison: ComparisonType,
    pub value: String,
}

impl Condition {
    pub fn new(source: String, comparison: ComparisonType, value: String) -> Self {
        Condition {
            source,
            comparison,
            value,
        }
    }
}

#[derive(Debug)]
pub struct SelectQuery {
    pub from: String,
    pub fields: Vec<FieldProjection>,
    pub group_by: GroupBy,
    pub where_constraints: Vec<Condition>,
    pub order_by_time: OrderDirection,
    pub limit: Option<u32>,
    pub slimit: Option<u32>,
}

#[derive(Debug, PartialEq)]
pub struct ShowFieldKeysQuery {
    pub from: String,
}

#[derive(Debug, PartialEq)]
pub struct ShowMeasurementsQuery {
    pub where_constraints: Vec<Condition>,
    pub limit: u32,
}

#[derive(Debug, PartialEq)]
pub struct ShowTagKeysQuery {
    pub from: String,
    pub where_constraints: Vec<Condition>,
}

#[derive(Debug, PartialEq)]
pub struct ShowTagValuesQuery {
    pub from: String,
    pub key: String,
    pub where_constraints: Vec<Condition>,
}

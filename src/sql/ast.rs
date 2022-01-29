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
}

#[derive(Debug, PartialEq)]
pub enum ComparisonType {
    Eq,
    NotEq,
    Gt,
    Lt,
    Like,
    NotLike,
}

#[derive(Debug)]
pub enum Time {
    NanoSeconds(u64),
    MicroSeconds(u64),
    MilliSeconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
    Days(u64),
}

#[derive(Debug)]
pub enum GroupBy {
    ByTime(Time),
    ByField(String),
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
    pub group_by: Vec<GroupBy>,
    pub where_constraints: Vec<Condition>,
    pub order_by_time: OrderDirection,
    pub limit: Option<u32>,
    pub slimit: Option<u32>,
}

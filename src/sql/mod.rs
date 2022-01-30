use ast::*;
use lalrpop_util::lalrpop_mod;

mod ast;

lalrpop_mod!(pub sqlparser, "/sql/parser.rs");

#[cfg(test)]
mod test {
    use super::*;
    use claim::{assert_none, assert_some};

    #[test]
    fn sql_parse() {
        let query = dbg!(sqlparser::SelectStatementParser::new().parse(
            //todo: this fails:
            // r#"SELECT "field_a", max("field_b") FROM "table_1" WHERE "field_a" = "field_b" AND "field_b" = "ololo" AND "field_a" =~ /^America$/ LIMIT 4"# 
            r#"
SELECT "field_a", max("field_b") FROM "table_1" WHERE ("field_a" = "field_b" AND ("field_b" = "ololo" AND "oss" = "moss")) AND "field_a" =~ /^America$/ GROUP BY time(5s), "ololo" fill(null) LIMIT 4"#
        ))
        .unwrap();
        assert_eq!(query.from, "table_1");
        assert_eq!(
            query.fields,
            vec![
                FieldProjection::simple("field_a"),
                FieldProjection::max("field_b"),
            ],
        );

        assert_eq!(
            query.where_constraints,
            vec![
                Condition::new(
                    "field_b".to_string(),
                    ComparisonType::Eq,
                    "ololo".to_string()
                ),
                Condition::new("oss".to_string(), ComparisonType::Eq, "moss".to_string()),
                Condition::new(
                    "field_a".to_string(),
                    ComparisonType::Eq,
                    "field_b".to_string()
                ),
                Condition::new(
                    "field_a".to_string(),
                    ComparisonType::Like,
                    "/^America$/".to_string()
                ),
            ],
        );

        assert_eq!(
            query.group_by,
            GroupBy {
                by_time: Some(Time::Seconds(5)),
                by_field: Some("ololo".into()),
                fill: Fill::Null
            }
        );

        assert_eq!(query.limit, Some(4u32));
    }

    #[test]
    fn sql_parse_real_query() {
        let query = dbg!(sqlparser::SelectStatementParser::new().parse(
            r#"
SELECT mean("value") FROM "logins.count" WHERE ("datacenter" =~ /^America$/ AND "hostname" =~ /^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/) AND time >= now() - 1h AND time <= now() GROUP BY time(1m), "hostname""#
        ))
        .unwrap();
        assert_eq!(query.from, "logins.count");
        assert_eq!(query.fields, vec![FieldProjection::mean("value"),],);

        assert_eq!(
            query.where_constraints,
            vec![
                Condition::new(
                    "datacenter".to_string(),
                    ComparisonType::Like,
                    "/^America$/".to_string()
                ),
                Condition::new(
                    "hostname".to_string(),
                    ComparisonType::Like,
                    r#"/^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/"#.to_string()
                ),
                Condition::new(
                    "time".to_string(),
                    ComparisonType::Gte,
                    "now() - 1h".to_string()
                ),
                Condition::new("time".to_string(), ComparisonType::Lte, "now()".to_string()),
            ],
        );

        assert_eq!(
            query.group_by,
            GroupBy {
                by_time: Some(Time::Minutes(1)),
                by_field: Some("hostname".to_string()),
                fill: Fill::None
            }
        );

        assert_none!(query.limit);
    }
}
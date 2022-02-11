use lalrpop_util::lalrpop_mod;

mod ast;
pub mod query_engine;

lalrpop_mod!(#[allow(clippy::all)] pub sqlparser, "/sql/parser.rs");

#[cfg(test)]
mod test {
    use super::*;
    use ast::*;
    use claim::assert_none;

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

    #[test]
    fn test_lowercase_keywords() {
        let query = dbg!(sqlparser::SelectStatementParser::new().parse(
            r#"
select mean("value") from "logins.count" where ("datacenter" =~ /^America$/ and "hostname" =~ /^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/) group by "hostname" limit 5 slimit 10"#
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
                )
            ],
        );

        assert_eq!(
            query.group_by,
            GroupBy {
                by_time: None,
                by_field: Some("hostname".to_string()),
                fill: Fill::None
            }
        );

        assert_eq!(Some(5), query.limit);
        assert_eq!(Some(10), query.slimit);
    }

    #[test]
    fn test_get_tag_values() {
        let query = dbg!(sqlparser::ShowTagValuesStatementParser::new().parse(
            r#"SHOW TAG VALUES FROM "cpu" WITH KEY = "hostname" WHERE "datacenter" =~ /^America$/ and "hostname" <> "server1""#
        ))
        .unwrap();
        assert_eq!(query.from, "cpu".to_string());
        assert_eq!(
            query.where_constraints,
            vec![
                Condition::new(
                    "datacenter".to_string(),
                    ComparisonType::Like,
                    "/^America$/".to_string(),
                ),
                Condition::new(
                    "hostname".to_string(),
                    ComparisonType::NotEq,
                    r#"server1"#.to_string(),
                ),
            ],
        );
    }

    #[test]
    fn test_get_tag_keys() {
        let query = dbg!(sqlparser::ShowTagKeysStatementParser::new().parse(
                r#"SHOW TAG KEYS FROM "cpu" WHERE "datacenter" =~ /^America$/ AND "hostname" !=~ /^(server1|server2)$/"# 
        ))
            .unwrap();
        assert_eq!(query.from, "cpu".to_string());
        assert_eq!(
            query.where_constraints,
            vec![
                Condition::new(
                    "datacenter".to_string(),
                    ComparisonType::Like,
                    "/^America$/".to_string(),
                ),
                Condition::new(
                    "hostname".to_string(),
                    ComparisonType::NotLike,
                    r#"/^(server1|server2)$/"#.to_string(),
                ),
            ],
        );
    }

    #[test]
    fn test_show_measurement() {
        let query = dbg!(sqlparser::ShowMeasurementsStatementParser::new().parse(
            r#"SHOW MEASUREMENTS WHERE "datacenter" =~ /^America$/ AND "hostname" =~ /^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/ LIMIT 100"#
        ))
        .unwrap();
        assert_eq!(query.limit, 100);
        assert_eq!(
            query.where_constraints,
            vec![
                Condition::new(
                    "datacenter".to_string(),
                    ComparisonType::Like,
                    "/^America$/".to_string(),
                ),
                Condition::new(
                    "hostname".to_string(),
                    ComparisonType::Like,
                    r#"/^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/"#.to_string(),
                ),
            ],
        );
    }

    #[test]
    fn test_query_parser() {
        let query = dbg!(sqlparser::QueryParser::new().parse(
            r#"SHOW MEASUREMENTS WHERE "datacenter" =~ /^America$/ AND "hostname" =~ /^(server1|server2|10\.1\.100\.1|10\.1\.100\.10)$/ LIMIT 100"#
        ))
        .unwrap();
        if let Query::Measurements(q) = query {
            assert_eq!(q.limit, 100);
        } else {
            panic!("Should be recognised as show measurements");
        }
    }
}

use ast::*;
use lalrpop_util::lalrpop_mod;

mod ast;

lalrpop_mod!(pub sqlparser, "/sql/parser.rs");

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sql_parse() {
        let query = dbg!(sqlparser::SelectStatementParser::new().parse(
            r#"SELECT "field_a", max("field_b") FROM "table_1" WHERE "field_a" = "field_b" AND "field_b" = "ololo""#
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
                    "field_a".to_string(),
                    ComparisonType::Eq,
                    "field_b".to_string()
                ),
                Condition::new(
                    "field_b".to_string(),
                    ComparisonType::Eq,
                    "ololo".to_string()
                ),
            ],
        );
    }
}

use fuzzcheck::mutators::grammar::*;
use std::rc::Rc;

fn series_name() -> Rc<Grammar> {
    regex("[a-z0-9]{2,10}")
}

fn kv() -> Rc<Grammar> {
    concatenation([
        regex("[a-z0-9]{1,10}"),
        literal('='),
        regex("[a-z0-9]{1,10}"),
    ])
}

fn kv_value() -> Rc<Grammar> {
    concatenation([
        kv(),
        repetition(concatenation([literal(','), kv()]), 0..=10),
    ])
}

fn timestamp() -> Rc<Grammar> {
    regex("[0-9]{1,8}")
}

fn influx_line() -> Rc<Grammar> {
    concatenation([
        series_name(),
        repetition(concatenation([literal(','), kv_value()]), 0..=1),
        literal(' '),
        kv_value(),
        literal(' '),
        timestamp(),
    ])
}

fn run_parsing(line: &str) -> bool {
    crate::protocol::Line::parse(line.as_bytes()).is_some()
}

#[cfg(not(feature = "no_fuzz"))]
// #[cfg_attr(not(feature = "no_fuzz"), ignore)]
#[cfg_attr(feature = "no_fuzz", ignore)]
// #[ignore]
#[test]
fn fuzz() {
    let mutator = grammar_based_ast_mutator(influx_line());
    let result = fuzzcheck::fuzz_test(|x: &AST| {
        let string = x.to_string();
        run_parsing(&string)
    })
    .mutator(mutator)
    .serde_serializer()
    .default_sensor_and_pool()
    .arguments_from_cargo_fuzzcheck()
    .stop_after_first_test_failure(true)
    .launch();
    assert!(!result.found_test_failure);
}

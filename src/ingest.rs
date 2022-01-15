use crate::protocol;
use crate::storage;
use crate::storage::Storage;
use std::collections::HashMap;
use std::rc::Rc;

struct Engine<T: Storage> {
    storage: T,
    metrics_cache: HashMap<String, Rc<str>>,
}

impl<T: Storage> Engine<T> {
    pub fn new(storage: T) -> Self {
        Engine {
            storage,
            metrics_cache: HashMap::new(),
        }
    }

    fn ingest(&mut self, line_str: &str) {
        if let Some(line) = protocol::Line::parse(line_str.as_bytes()) {
            for (field_name, field_value) in line.fields_iter() {
                if let Ok(int_value) = field_value.parse::<i64>() {
                    let name = format!("{}:{}", line.timeseries_name(), field_name);
                    let rc_name = self
                        .metrics_cache
                        .entry(name.clone())
                        .or_insert_with(|| Rc::from(name));
                    let data_point =
                        storage::DataPoint::new(rc_name.clone(), line.timestamp, int_value);
                    self.storage.add(data_point);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_test() {
        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::new(storage);
        let line_str =
            "weather,location=us-midwest,country=us temperature=0,humidity=1 1465839830100400200";
        engine.ingest(&line_str);
        let line2_str =
            "weather,location=us-midwest,country=us temperature=2,humidity=3 1465839830100400201";
        engine.ingest(&line2_str);
        let metrics = engine.storage.load("weather:temperature");

        assert_eq!(
            metrics
                .into_iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(i64, u64)>>(),
            vec![(0, 1465839830100400200), (2, 1465839830100400201)]
        );
    }
}

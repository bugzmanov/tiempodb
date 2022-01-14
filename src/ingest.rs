use crate::protocol;
use crate::storage;
use crate::storage::Storage;
use std::rc::Rc;

fn ingest() {
    let mut storage = storage::SnaphotableStorage::new();
    let line_str = "";
    if let Some(line) = protocol::Line::parse(line_str.as_bytes()) {
        for (field_name, field_value) in line.fields_iter() {
            if let Ok(int_value) = field_value.parse::<i64>() {
                let name = format!("{}:{}", line.timeseries_name(), field_name);
                let data_point = storage::DataPoint::new(Rc::from(name), line.timestamp, int_value);
                storage.add(data_point);
            }
        }
    }
}

use fake::{Dummy, Fake, Faker};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::rc::Rc;

struct FakeRc;

impl Dummy<FakeRc> for Rc<str> {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &FakeRc, rng: &mut R) -> Rc<str> {
        Rc::from(format!("timeseries_{}", rng.gen::<u32>()))
    }
}

#[derive(Clone, Debug, Eq, Dummy)]
struct DataPoint {
    #[dummy(faker = "FakeRc")]
    name: Rc<str>,
    timestamp: u64,
    value: i64,
}

impl PartialEq for DataPoint {
    fn eq(&self, other: &DataPoint) -> bool {
        self.name == other.name && self.timestamp == other.timestamp && self.value == other.value
    }
}

trait Storage {
    fn add(&mut self, point: DataPoint);
    fn add_bulk(&mut self, points: &[DataPoint]);
    fn load(&mut self, metric_name: &str) -> Vec<DataPoint>;
}

struct MemoryStorage {
    map: HashMap<Rc<str>, Vec<DataPoint>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            map: HashMap::new(),
        }
    }

    fn create_for_key(&mut self, key: &Rc<str>) -> &mut Vec<DataPoint> {
        self.map.insert(key.clone(), Vec::new());
        self.map.get_mut(key).expect("getting after insert")
    }
}

impl Storage for MemoryStorage {
    fn add(&mut self, point: DataPoint) {
        if let Some(values) = self.map.get_mut(&point.name) {
            values.push(point);
        } else {
            self.map.insert(point.name.clone(), vec![point]);
        }
    }
    fn add_bulk(&mut self, points: &[DataPoint]) {
        if points.len() == 0 {
            return;
        }
        let mut curr = &points[0].name;
        // let mut vector = self.map.get_mut(&points[0].name);
        let mut vector = match self.map.get_mut(&points[0].name) {
            Some(vec) => vec,
            None => self.create_for_key(curr),
        };

        for point in points.into_iter() {
            if &point.name == curr {
                vector.push(point.clone());
            } else {
                curr = &point.name;
                vector = self.create_for_key(curr);
                vector.push(point.clone());
            }
        }
    }

    fn load(&mut self, metric_name: &str) -> Vec<DataPoint> {
        if let Some(data) = self.map.get_mut(metric_name) {
            data.sort_by_key(|metric| metric.timestamp);
            return data.to_vec();
        } else {
            return Vec::new();
        }
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod test {
    use super::*;
    // use claim::assert_none;
    // use claim::assert_

    const METRIC_NAME: &str = "ololoev";

    fn data_point(metric: &str) -> DataPoint {
        let mut point = Faker.fake::<DataPoint>();
        point.name = Rc::from(metric);
        point
    }

    #[test]
    fn test_non_existing_series() {
        let mut storage = MemoryStorage::new();
        let point = data_point(METRIC_NAME);
        storage.add(point);
        assert!(storage.load("something_else").is_empty());
    }

    #[test]
    fn test_loading_existing_metric() {
        let mut storage = MemoryStorage::new();
        let point = data_point(METRIC_NAME);
        storage.add(point.clone());
        let loaded = storage.load(METRIC_NAME);
        assert_eq!(loaded, vec![point]);
    }

    fn is_ordered_by_time(points: &[DataPoint]) -> bool {
        if points.len() <= 1 {
            return true;
        }
        for i in 1..points.len() {
            if points[i - 1].timestamp > points[i].timestamp {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_loaded_metrics_must_be_sorted() {
        let mut storage = MemoryStorage::new();
        let mut data_points = fake::vec![DataPoint; 4];
        let metric: Rc<str> = Rc::from(METRIC_NAME);
        for point in &mut data_points {
            point.name = metric.clone();
        }
        storage.add_bulk(&data_points);

        let result = storage.load(METRIC_NAME);

        assert_eq!(4, result.len());
        assert_eq!(true, is_ordered_by_time(&result));
    }
}

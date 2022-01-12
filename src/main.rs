use fake::{Dummy, Fake, Faker};
use rand::Rng;
use std::collections::HashMap;
use std::rc::Rc;

mod protocol;

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

impl Default for MemoryStorage {
    fn default() -> Self {
        MemoryStorage::new()
    }
}

struct SnaphotableStorage {
    snapshot: MemoryStorage,
    active: MemoryStorage,
}

impl SnaphotableStorage {
    fn new() -> Self {
        SnaphotableStorage {
            snapshot: MemoryStorage::new(),
            active: MemoryStorage::new(),
        }
    }

    fn make_snapshot(&mut self) {
        let curr = std::mem::take(&mut self.active);
        self.snapshot = curr;
    }
}

impl Storage for SnaphotableStorage {
    fn add(&mut self, point: DataPoint) {
        self.active.add(point);
    }

    fn add_bulk(&mut self, points: &[DataPoint]) {
        self.active.add_bulk(points);
    }

    fn load(&mut self, metric_name: &str) -> Vec<DataPoint> {
        let snapshot_vec = self.snapshot.load(metric_name);
        let active_vec = self.active.load(metric_name);
        if snapshot_vec.is_empty() {
            return active_vec;
        }

        if active_vec.is_empty() {
            return snapshot_vec;
        }

        let mut result = Vec::with_capacity(snapshot_vec.len() + active_vec.len());

        let mut left_iter = snapshot_vec.into_iter();
        let mut right_iter = active_vec.into_iter();

        let mut left = left_iter.next();
        let mut right = right_iter.next();

        while left.is_some() || right.is_some() {
            match (&mut left, &mut right) {
                (&mut None, &mut None) => {} //do nothing
                (left_item @ &mut Some(_), _r @ &mut None) => {
                    result.push(left_item.take().expect("shuld not happen"))
                }
                (_l @ &mut None, right_item @ &mut Some(_)) => {
                    result.push(right_item.take().expect("should not hppen"))
                }
                (left_item @ &mut Some(_), right_item @ &mut Some(_)) => {
                    if left_item.as_ref().expect("").timestamp
                        > right_item.as_ref().expect("").timestamp
                    {
                        result.push(right_item.take().expect("shuld not happen"));
                    } else {
                        result.push(left_item.take().expect("shuld not happen"));
                    }
                }
            }

            if left.is_none() {
                left = left_iter.next();
            }

            if right.is_none() {
                right = right_iter.next();
            }
        }

        result
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

    #[test]
    fn snapshottable_should_return_ordered_from_both_sources() {
        let mut storage = SnaphotableStorage::new();
        let mut data_points = fake::vec![DataPoint; 4];
        let metric: Rc<str> = Rc::from(METRIC_NAME);
        for point in &mut data_points {
            point.name = metric.clone();
        }
        storage.add_bulk(&data_points);
        let mut second_batch = fake::vec![DataPoint; 3];
        let metric: Rc<str> = Rc::from(METRIC_NAME);
        for point in &mut second_batch {
            point.name = metric.clone();
        }

        storage.make_snapshot();

        storage.add_bulk(&second_batch);
        assert_eq!(4, storage.snapshot.load(METRIC_NAME).len());
        assert_eq!(3, storage.active.load(METRIC_NAME).len());

        let result = storage.load(METRIC_NAME);

        assert_eq!(7, result.len());
        assert_eq!(true, is_ordered_by_time(&result));
    }
}

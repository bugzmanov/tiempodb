use core::marker::PhantomData;
use core::ops::Deref;
use fake::{Dummy, Fake};
use parking_lot::lock_api::RawRwLock;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

struct FakeRc;

impl Dummy<FakeRc> for Arc<str> {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &FakeRc, rng: &mut R) -> Arc<str> {
        Arc::from(format!("timeseries_{}", rng.gen::<u32>()))
    }
}

#[derive(Clone, Debug, Eq, Dummy)]
pub struct DataPoint {
    #[dummy(faker = "FakeRc")]
    pub name: Arc<str>,
    pub timestamp: u64,
    pub value: i64,
}

struct VecHold<'a>(Vec<&'a DataPoint>);

impl DataPoint {
    pub fn new(name: Arc<str>, timestamp: u64, value: i64) -> Self {
        DataPoint {
            name,
            timestamp,
            value,
        }
    }
}

impl PartialEq for DataPoint {
    fn eq(&self, other: &DataPoint) -> bool {
        self.name == other.name && self.timestamp == other.timestamp && self.value == other.value
    }
}

pub trait Storage {
    fn add(&mut self, point: DataPoint);
    fn add_bulk(&mut self, points: &[DataPoint]);
    fn load_unsafe(&self, metric_name: &str) -> Vec<&DataPoint>;
    fn load<'a>(
        &'a self,
        metric_name: &str,
    ) -> Box<dyn std::ops::Deref<Target = Vec<&'a DataPoint>> + 'a>;
    fn active_set_size(&self) -> usize;
}

impl<'a> std::ops::Deref for VecHold<'a> {
    type Target = Vec<&'a DataPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Storage for HashMap<Arc<str>, Vec<DataPoint>> {
    fn add(&mut self, point: DataPoint) {
        if let Some(values) = self.get_mut(&point.name) {
            values.push(point);
        } else {
            self.insert(point.name.clone(), vec![point]);
        }
    }

    fn add_bulk(&mut self, points: &[DataPoint]) {
        if points.is_empty() {
            return;
        }
        let mut curr = &points[0].name;
        // let mut vector = self.map.get_mut(&points[0].name);
        let mut vector = match self.get_mut(&points[0].name) {
            Some(vec) => vec,
            None => create_for_key(self, curr),
        };

        for point in points.iter() {
            if &point.name == curr {
                vector.push(point.clone());
            } else {
                curr = &point.name;
                vector = create_for_key(self, curr);
                vector.push(point.clone());
            }
        }
    }

    fn load_unsafe(&self, metric_name: &str) -> Vec<&DataPoint> {
        if let Some(data) = self.get(metric_name) {
            let mut result: Vec<&DataPoint> = data.iter().collect();
            result.sort_by_key(|metric| metric.timestamp);
            result
        } else {
            // todo!("saasda")
            Vec::new()
        }
    }

    // fn load(&self, metric_name: &str) -> Vec<&DataPoint> {
    fn load<'a>(
        &'a self,
        metric_name: &'_ str,
    ) -> Box<dyn std::ops::Deref<Target = Vec<&'a DataPoint>> + 'a> {
        if let Some(data) = self.get(metric_name) {
            let mut result: Vec<&DataPoint> = data.iter().collect();
            result.sort_by_key(|metric| metric.timestamp);
            Box::new(Box::new(result))
        } else {
            // todo!("saasda")
            Box::new(Box::new(Vec::new()))
        }
    }

    fn active_set_size(&self) -> usize {
        todo!("unimplemented")
    }
}

#[derive(Default)]
pub struct MemoryStorage {
    stat: StorageStat,
    map: HashMap<Arc<str>, Vec<DataPoint>>,
}

fn create_for_key<'a, T: Default>(
    map: &'a mut HashMap<Arc<str>, T>,
    key: &'_ Arc<str>,
) -> &'a mut T {
    map.insert(key.clone(), T::default());
    map.get_mut(key).expect("getting after insert")
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            map: HashMap::default(),
            stat: StorageStat::default(),
        }
    }

    // fn create_for_key(&mut self, key: &Rc<str>) -> &mut Vec<DataPoint> {
    //     self.map.insert(key.clone(), Vec::new());
    //     self.map.get_mut(key).expect("getting after insert")
    // }
}

#[derive(Default)]
pub struct StorageStat {
    data_points_count: usize,
}

impl Storage for MemoryStorage {
    fn add(&mut self, point: DataPoint) {
        self.map.add(point);
        self.stat.data_points_count += 1;
    }
    fn add_bulk(&mut self, points: &[DataPoint]) {
        self.map.add_bulk(points);
        self.stat.data_points_count += points.len();
    }

    fn load(&self, metric_name: &str) -> Box<dyn std::ops::Deref<Target = Vec<&DataPoint>> + '_> {
        self.map.load(metric_name)
    }

    fn load_unsafe(&self, metric_name: &str) -> Vec<&DataPoint> {
        self.map.load_unsafe(metric_name)
    }

    fn active_set_size(&self) -> usize {
        self.stat.data_points_count
    }
}

#[derive(Default)]
pub struct SnaphotableStorage {
    snapshot: Arc<RwLock<HashMap<Arc<str>, Vec<DataPoint>>>>,
    active: MemoryStorage,
}

pub struct OwningReadGuard<'a, R: RawRwLock, T: ?Sized> {
    raw: &'a R,
    data: Vec<&'a T>,
    marker: PhantomData<Vec<&'a T>>,
}

impl<'a, R: RawRwLock, T> OwningReadGuard<'a, R, T> {
    fn with_data(&mut self, new_data: Vec<&'a T>) {
        self.data = new_data
    }
}

impl<'a, R: RawRwLock + 'a, T: ?Sized + 'a> Deref for OwningReadGuard<'a, R, T> {
    type Target = Vec<&'a T>;
    #[inline]
    fn deref(&self) -> &Vec<&'a T> {
        &self.data
    }
}

impl<'a, R: RawRwLock + 'a, T: ?Sized + 'a> Drop for OwningReadGuard<'a, R, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            self.raw.unlock_shared();
        }
    }
}

impl SnaphotableStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn make_snapshot(&mut self) {
        let curr = std::mem::take(&mut self.active);
        let mut write = self.snapshot.write();
        let _ = std::mem::replace(&mut *write, curr.map);
    }

    pub fn snapshot(&self) -> parking_lot::RwLockReadGuard<'_, HashMap<Arc<str>, Vec<DataPoint>>> {
        self.snapshot.read()
    }

    pub fn share_snapshot(&self) -> Arc<RwLock<HashMap<Arc<str>, Vec<DataPoint>>>> {
        self.snapshot.clone()
    }

    fn load_from_snapshot(
        &self,
        metric_name: &str,
    ) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint> {
        unsafe { self.snapshot.raw().lock_shared() };
        let data = unsafe { &*self.snapshot.data_ptr() };
        let points = data.load_unsafe(metric_name);

        OwningReadGuard {
            raw: unsafe { self.snapshot.raw() },
            data: points,
            marker: PhantomData,
        }
    }

    pub fn load_locked(
        &self,
        metric_name: &str,
    ) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint> {
        let mut snapshot_vec = self.load_from_snapshot(metric_name);

        let active_vec = self.active.load_unsafe(metric_name);
        if snapshot_vec.is_empty() {
            snapshot_vec.with_data(active_vec);
            return snapshot_vec; //todo: don't need to hold the lock at this point
        }

        if active_vec.is_empty() {
            return snapshot_vec;
        }

        let mut result = Vec::with_capacity(snapshot_vec.len() + active_vec.len());

        let mut left_iter = snapshot_vec.iter();
        let mut right_iter = active_vec.into_iter();

        let mut left = left_iter.next();
        let mut right = right_iter.next();

        while left.is_some() || right.is_some() {
            match (&mut left, &mut right) {
                (&mut None, &mut None) => {} //do nothing
                (left_item @ &mut Some(_), _r @ &mut None) => {
                    result.push(*left_item.take().expect("shuld not happen"))
                }
                (_l @ &mut None, right_item @ &mut Some(_)) => {
                    result.push(right_item.take().expect("should not hppen"))
                }
                (left_item @ &mut Some(_), right_item @ &mut Some(_)) => {
                    if left_item.expect("").timestamp > right_item.as_ref().expect("").timestamp {
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

        snapshot_vec.with_data(result);
        snapshot_vec
    }
}

impl Storage for SnaphotableStorage {
    fn add(&mut self, point: DataPoint) {
        self.active.add(point);
    }

    fn add_bulk(&mut self, points: &[DataPoint]) {
        self.active.add_bulk(points);
    }

    fn load<'a>(
        &'a self,
        metric_name: &str,
    ) -> Box<dyn std::ops::Deref<Target = Vec<&'a DataPoint>> + 'a> {
        Box::new(self.load_locked(metric_name))
    }

    fn active_set_size(&self) -> usize {
        self.active.active_set_size()
    }

    fn load_unsafe(&self, metric_name: &str) -> Vec<&DataPoint> {
        let read = self.snapshot.read();
        let data = unsafe { &*self.snapshot.data_ptr() };
        drop(read);

        let snapshot_vec = data.load_unsafe(metric_name);

        let active_vec = self.active.load_unsafe(metric_name);
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
                    if left_item.expect("").timestamp > right_item.as_ref().expect("").timestamp {
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

#[cfg(test)]
mod test {
    use super::*;
    use claim::{assert_none, assert_some};
    use fake::Faker;
    // use claim::assert_none;
    // use claim::assert_

    const METRIC_NAME: &str = "ololoev";

    fn data_point(metric: &str) -> DataPoint {
        let mut point = Faker.fake::<DataPoint>();
        point.name = Arc::from(metric);
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
        assert_eq!(***loaded, vec![&point]);
    }

    fn is_ordered_by_time(points: &[&DataPoint]) -> bool {
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
        storage.add_bulk(&generate_data_points(METRIC_NAME, 4));

        let result = storage.load(METRIC_NAME);

        assert_eq!(4, result.len());
        assert_eq!(true, is_ordered_by_time(&result));
    }

    #[test]
    fn snapshottable_should_return_ordered_from_both_sources() {
        let mut storage = SnaphotableStorage::new();
        storage.add_bulk(&generate_data_points(METRIC_NAME, 4));

        storage.make_snapshot();

        storage.add_bulk(&generate_data_points(METRIC_NAME, 3));

        assert_eq!(3, storage.active.load(METRIC_NAME).len());

        let result = storage.load_locked(METRIC_NAME);

        assert_eq!(7, result.len());
        assert_eq!(true, is_ordered_by_time(&result));
    }

    #[test]
    fn test_owning_read_guard() {
        let mut storage = SnaphotableStorage::new();
        storage.add_bulk(&generate_data_points(METRIC_NAME, 4));

        storage.make_snapshot();

        let read_guard = storage.load_from_snapshot(METRIC_NAME);
        assert_none!(storage.snapshot.try_write());

        assert_eq!(read_guard.len(), 4);

        drop(read_guard);
        assert_some!(storage.snapshot.try_write());
    }

    fn generate_data_points(metric_name: &str, size: usize) -> Vec<DataPoint> {
        let mut data_points = fake::vec![DataPoint; size];
        let metric: Arc<str> = Arc::from(metric_name);
        for point in &mut data_points {
            point.name = metric.clone();
        }
        data_points
    }

    #[test]
    fn read_from_two_sources_keeps_the_lock() {
        let mut storage = SnaphotableStorage::new();
        storage.add_bulk(&generate_data_points(METRIC_NAME, 4));

        storage.make_snapshot();

        storage.add_bulk(&generate_data_points(METRIC_NAME, 3));

        let result = storage.load_locked(METRIC_NAME);
        assert_eq!(7, result.len());
        assert_eq!(true, is_ordered_by_time(&result));

        assert_none!(storage.snapshot.try_write());
    }
}

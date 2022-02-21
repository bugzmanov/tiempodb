use core::marker::PhantomData;
use core::ops::Deref;
use crossbeam::channel;
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

#[derive(Clone, Debug, Dummy)]
pub struct DataPoint {
    #[dummy(faker = "FakeRc")]
    pub name: Arc<str>,
    pub timestamp: u64,
    pub value: f64,
}

impl DataPoint {
    pub fn new(name: Arc<str>, timestamp: u64, value: f64) -> Self {
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

pub type MetricsData = HashMap<Arc<str>, Vec<DataPoint>>;

pub trait StorageWriter {
    fn add(&mut self, point: DataPoint);
    fn add_bulk(&mut self, points: &[DataPoint]);
}

pub trait StorageReader {
    fn load(&self, metric_name: &str) -> Vec<&DataPoint>;
}

pub trait ProtectedStorageReader {
    fn read_metrics(
        &self,
        metric_name: &str,
    ) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint>;
}

impl StorageWriter for MetricsData {
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
}

impl StorageReader for MetricsData {
    fn load(&self, metric_name: &str) -> Vec<&DataPoint> {
        if let Some(data) = self.get(metric_name) {
            let mut result: Vec<&DataPoint> = data.iter().collect();
            result.sort_by_key(|metric| metric.timestamp);
            result
        } else {
            Vec::new()
        }
    }
}

#[derive(Default)]
pub struct StorageStat {
    data_points_count: usize,
}

#[derive(Default)]
pub struct MemoryStorage {
    stat: StorageStat,
    map: MetricsData,
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

    pub fn active_set_size(&self) -> usize {
        self.stat.data_points_count
    }

    fn load(&self, metric_name: &str) -> Vec<&DataPoint> {
        self.map.load(metric_name)
    }
}

impl StorageWriter for MemoryStorage {
    fn add(&mut self, point: DataPoint) {
        self.map.add(point);
        self.stat.data_points_count += 1;
    }
    fn add_bulk(&mut self, points: &[DataPoint]) {
        self.map.add_bulk(points);
        self.stat.data_points_count += points.len();
    }
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

pub struct SnaphotableStorage {
    metrics_snap: Arc<RwLock<MetricsData>>,
    active: MemoryStorage,
    outbox: crossbeam::channel::Sender<MetricsData>,

    #[cfg(test)]
    snapshot: StorageSnapshot,
}

impl SnaphotableStorage {
    pub fn new() -> Self {
        let (tasks_sender, tasks_receiver) = crossbeam::channel::unbounded();
        let snapshot = StorageSnapshot::new(tasks_receiver);
        let snap = snapshot.snapshot.clone();

        #[cfg(not(test))]
        std::thread::spawn(move || {
            snapshot.run();
        });

        SnaphotableStorage {
            metrics_snap: snap,
            active: MemoryStorage::default(),
            outbox: tasks_sender,
            #[cfg(test)]
            snapshot: snapshot,
        }
    }

    pub fn make_snapshot(&mut self) {
        let curr = std::mem::take(&mut self.active);
        self.outbox.send(curr.map).unwrap(); //todo unwrap
        #[cfg(test)]
        self.snapshot.tick().unwrap(); //todo unwrap
    }

    pub fn share_snapshot(&self) -> Arc<RwLock<HashMap<Arc<str>, Vec<DataPoint>>>> {
        self.metrics_snap.clone()
    }

    #[cfg(test)]
    pub fn load_from_snapshot(
        &self,
        metric_name: &str,
    ) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint> {
        self.snapshot.read(metric_name)
    }

    pub fn active_set_size(&self) -> usize {
        self.active.active_set_size()
    }

    pub fn snapshot_set_size(&self) -> usize {
        (*self.metrics_snap.read()).len()
    }
}

impl StorageWriter for SnaphotableStorage {
    fn add(&mut self, point: DataPoint) {
        self.active.add(point);
    }

    fn add_bulk(&mut self, points: &[DataPoint]) {
        self.active.add_bulk(points);
    }
}

pub struct StorageSnapshot {
    snapshot: Arc<RwLock<MetricsData>>,
    inbox: channel::Receiver<MetricsData>,
}

impl ProtectedStorageReader for RwLock<MetricsData> {
    fn read_metrics(
        &self,
        metric_name: &str,
    ) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint> {
        unsafe { self.raw().lock_shared() };
        let data = unsafe { &*self.data_ptr() };
        let points = data.load(metric_name);

        OwningReadGuard {
            raw: unsafe { self.raw() },
            data: points,
            marker: PhantomData,
        }
    }
}

impl StorageSnapshot {
    fn new(inbox: channel::Receiver<MetricsData>) -> Self {
        StorageSnapshot {
            snapshot: Arc::new(RwLock::new(MetricsData::default())),
            inbox,
        }
    }

    pub fn run(&self) {
        loop {
            self.tick().unwrap(); //todo: unwrap
        }
    }
    pub fn tick(&self) -> anyhow::Result<()> {
        let mut data = self.inbox.recv()?;
        let mut write = self.snapshot.write();
        StorageSnapshot::merge_to_right(&mut data, &mut *write);
        Ok(())
    }

    fn merge_to_right(left: &mut MetricsData, right: &mut MetricsData) {
        for (k, mut v) in left.into_iter() {
            if let Some(list) = right.get_mut(k) {
                list.append(&mut v);
                list.sort_by_key(|m| m.timestamp); // todo: not sure if we need sorting that early
            } else {
                right.insert(k.clone(), v.drain(..).collect());
            }
        }
    }

    fn read(&self, metric_name: &str) -> OwningReadGuard<'_, parking_lot::RawRwLock, DataPoint> {
        self.snapshot.read_metrics(metric_name)
        // unsafe { self.snapshot.raw().lock_shared() };
        // let data = unsafe { &*self.snapshot.data_ptr() };
        // let points = data.load(metric_name);

        // OwningReadGuard {
        //     raw: unsafe { self.snapshot.raw() },
        //     data: points,
        //     marker: PhantomData,
        // }
    }

    pub fn active_set_size(&self) -> usize {
        (*self.snapshot.read()).len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use claim::{assert_none, assert_some};
    use fake::Faker;

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
        assert_eq!(loaded, vec![&point]);
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
    fn snapshottable_should_return_ordered_from_both_sources() {
        let mut storage = SnaphotableStorage::new();
        storage.add_bulk(&generate_data_points(METRIC_NAME, 4));
        storage.add_bulk(&generate_data_points(METRIC_NAME, 3));

        storage.make_snapshot();

        let result = storage.load_from_snapshot(METRIC_NAME);

        assert_eq!(7, result.len());
        assert_eq!(true, is_ordered_by_time(&result));
    }

    #[test]
    fn test_owning_read_guard() {
        let (tasks_sender, tasks_receiver) = crossbeam::channel::unbounded();
        let snapshot = StorageSnapshot::new(tasks_receiver);
        let mut data = HashMap::new();
        data.insert(Arc::from(METRIC_NAME), generate_data_points(METRIC_NAME, 4));
        tasks_sender.send(data).unwrap();

        snapshot.tick().unwrap();

        let read_guard = snapshot.read(METRIC_NAME);
        assert_none!(snapshot.snapshot.try_write());

        assert_eq!(read_guard.len(), 4);

        drop(read_guard);

        let rw_lock = snapshot.snapshot.try_write();
        assert_some!(&rw_lock);
    }

    fn generate_data_points(metric_name: &str, size: usize) -> Vec<DataPoint> {
        let mut data_points = fake::vec![DataPoint; size];
        let metric: Arc<str> = Arc::from(metric_name);
        for point in &mut data_points {
            point.name = metric.clone();
        }
        data_points
    }
}

use crate::partition::PartitionManager;
use crate::protocol;
use crate::storage;
use crate::storage::DataPoint;
use crate::storage::SnaphotableStorage;
use crate::storage::StorageWriter;
use crate::wal::Wal;
use crate::wal::WalBlockReader;
use anyhow::Result;
use crossbeam::channel;
use crossbeam::channel::SendError;
use crossbeam::channel::TryRecvError;
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use streaming_iterator::StreamingIterator;

struct SnapshotProgress {
    inbox: channel::Receiver<usize>,
    outbox: channel::Sender<usize>,
    pending: bool,
}

impl SnapshotProgress {
    fn new(inbox: channel::Receiver<usize>, outbox: channel::Sender<usize>) -> Self {
        SnapshotProgress {
            inbox,
            outbox,
            pending: false,
        }
    }

    fn persist_snapshot(
        &mut self,
        wall_log_position: usize,
        storage: &mut SnaphotableStorage,
    ) -> Result<bool, SendError<usize>> {
        if self.pending {
            Ok(false)
        } else {
            storage.make_snapshot();
            self.outbox.send(wall_log_position)?;
            self.pending = true;
            Ok(true)
        }
    }

    fn can_truncate_wal(&mut self) -> Result<Option<usize>, TryRecvError> {
        match self.inbox.try_recv() {
            Ok(usize) => {
                self.pending = false;
                Ok(Some(usize))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct Engine {
    storage: SnaphotableStorage,
    metrics_cache: HashMap<String, Arc<str>>,
    wal: Wal,
    snapshot_progress: SnapshotProgress,
    snapshot_wal_position: usize,
    #[cfg(test)]
    worker: PartitionWorker,
}

impl Engine {
    pub fn new(
        storage: SnaphotableStorage,
        wal_path: &Path,
        partitions_path: &Path,
    ) -> Result<Self> {
        let (tasks_sender, tasks_receiver) = crossbeam::channel::unbounded();
        let (results_sender, results_receiver) = crossbeam::channel::unbounded();

        let manager = PartitionManager::new(partitions_path)?;

        #[allow(unused_mut)]
        let mut worker = PartitionWorker::new(
            tasks_receiver,
            results_sender,
            manager,
            storage.share_snapshot(),
        );

        #[cfg(not(test))]
        std::thread::spawn(move || worker.run());

        let snapshot_progress = SnapshotProgress::new(results_receiver, tasks_sender);
        let snapshot_wal_position = 0usize;

        Ok(Engine {
            storage,
            metrics_cache: HashMap::new(),
            wal: Wal::new(wal_path)?,
            snapshot_progress,
            snapshot_wal_position,
            #[cfg(test)]
            worker,
        })
    }

    //todo: bad place for this
    pub fn time_tick(&mut self) {
        self.storage.make_snapshot();
    }

    //todo: ingest multi-line
    pub fn ingest(&mut self, line_str: &str) -> Result<()> {
        self.wal.write(line_str.as_bytes())?;
        self.save_to_storage(line_str);
        //todo: hardcoded value for now
        if self.storage.active_set_size() > 100 {
            match self
                .snapshot_progress
                .persist_snapshot(self.snapshot_wal_position, &mut self.storage)
            {
                Ok(false) => {} /* do nothing */
                Ok(true) => {
                    self.snapshot_wal_position =
                        self.wal.roll_new_segment(self.snapshot_wal_position)? as usize;
                    #[cfg(test)]
                    assert!(self.worker.tick());
                }
                Err(e) => {
                    log::error!(
                        "[ingest engine] Failed to request to persist snapshot: {}",
                        e
                    );
                    //todo: not sure if panic is the best way out. but it looks like irrecoverable situation
                    panic!("Failed to request to persist snapshot. This might indicate that persistent thread is dead. Reason:{}", e);
                }
            }
        }
        if let Ok(Some(pos)) = self.snapshot_progress.can_truncate_wal() {
            self.wal.drop_pending(pos as u64)?; //todo: usize vs u64
        }
        Ok(())
    }

    fn save_to_storage(&mut self, line_str: &str) {
        if let Some(line) = protocol::Line::parse(line_str.as_bytes()) {
            let tags = line.tags();

            for (field_name, field_value) in line.fields_iter() {
                if let Ok(int_value) = field_value.parse::<f64>() {
                    let name = format!("{}:{}", line.timeseries_name(), field_name);
                    let rc_name = self
                        .metrics_cache
                        .entry(name.clone()) //todo: clone?
                        .or_insert_with(|| Arc::from(name));
                    let mut data_point =
                        storage::DataPoint::new(rc_name.clone(), line.timestamp, int_value);
                    data_point.set_tags(&tags);
                    self.storage.add(data_point);
                } else {
                    log::error!("failed to parse {}", line_str);
                }
            }
        } else {
            log::error!("Failed to parse {}", line_str);
        }
    }

    pub fn restore_from_wal(
        storage: SnaphotableStorage,
        wal_path: &Path,
        partitions_path: &Path,
    ) -> Result<Self> {
        let mut iter = WalBlockReader::read(wal_path)?.into_iter();
        let mut storage = Engine::new(storage, wal_path, partitions_path)?;
        loop {
            iter.advance();
            match iter.get() {
                Some(v) => {
                    let str_block = unsafe { String::from_utf8_unchecked(Vec::from(v)) };
                    for str in str_block.split('\n') {
                        storage.save_to_storage(str)
                    }
                }
                None => break,
            }
        }
        storage
            .wal
            .truncate(iter.last_successfull_read_position())?;

        Ok(storage)
    }
}

struct PartitionWorker {
    inbox: channel::Receiver<usize>,
    outbox: channel::Sender<usize>,
    partition_manager: PartitionManager,
    snapshot: Arc<RwLock<HashMap<Arc<str>, Vec<DataPoint>>>>,
}

impl PartitionWorker {
    fn new(
        inbox: channel::Receiver<usize>,
        outbox: channel::Sender<usize>,
        partition_manager: PartitionManager,
        snapshot: Arc<RwLock<HashMap<Arc<str>, Vec<DataPoint>>>>,
    ) -> Self {
        PartitionWorker {
            inbox,
            outbox,
            partition_manager,
            snapshot,
        }
    }

    #[cfg(not(test))]
    pub fn run(&mut self) {
        while self.tick() {}
        log::info!("PartitionWorker shutdown, because inbox channel became disconnected")
    }

    fn tick(&mut self) -> bool {
        match self.inbox.recv() {
            Ok(position) => {
                if self.roll_partition().is_ok() {
                    self.outbox.send(position).unwrap(); //todo: handle failure
                } else {
                    self.outbox.send(0).unwrap(); //todo handle failure
                }
                true
            }
            Err(e) => {
                dbg!(e);
                false
                // todo!("handle failure")
            }
        }
    }

    fn roll_partition(&mut self) -> Result<()> {
        let r = self.snapshot.upgradable_read();
        // let r = self.snapshot.read();
        self.partition_manager.roll_new_partition(&*r)?;
        let mut w = RwLockUpgradableReadGuard::<
            '_,
            parking_lot::RawRwLock,
            HashMap<Arc<str>, Vec<DataPoint>>,
        >::upgrade(r);
        std::mem::take(&mut *w);
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn simple_test() -> Result<()> {
        let file = tempfile::NamedTempFile::new().unwrap();
        let tempdir = tempfile::tempdir().unwrap();

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::new(storage, file.path(), tempdir.path())?;
        let line_str =
            "weather,location=us-midwest,country=us temperature=0,humidity=1 1465839830100400200";
        engine.ingest(&line_str)?;
        let line2_str =
            "weather,location=us-midwest,country=us temperature=2,humidity=3 1465839830100400201";
        engine.ingest(&line2_str)?;

        engine.storage.make_snapshot();
        let metrics = engine.storage.load_from_snapshot("weather:temperature");

        assert_eq!(
            metrics
                .iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(f64, u64)>>(),
            vec![(0f64, 1465839830100400200), (2f64, 1465839830100400201)]
        );
        Ok(())
    }

    #[test]
    fn test_restore_from_wal() -> Result<()> {
        let file = tempfile::NamedTempFile::new().unwrap();
        let tempdir = tempfile::tempdir().unwrap();

        let mut storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::new(storage, file.path(), tempdir.path())?;
        let line_str =
            "weather,location=us-midwest,country=us temperature=0,humidity=1 1465839830100400200";
        engine.ingest(&line_str)?;
        let line2_str =
            "weather,location=us-midwest,country=us temperature=2,humidity=3 1465839830100400201";
        engine.ingest(&line2_str)?;

        storage = storage::SnaphotableStorage::new();
        engine = Engine::restore_from_wal(storage, file.path(), tempdir.path())?;

        engine.storage.make_snapshot();
        let metrics = engine.storage.load_from_snapshot("weather:temperature");

        assert_eq!(
            metrics
                .iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(f64, u64)>>(),
            vec![(0f64, 1465839830100400200), (2f64, 1465839830100400201)]
        );
        Ok(())
    }

    #[test]
    fn test_restore_from_corrupt_wall() -> Result<()> {
        let file = tempfile::NamedTempFile::new().unwrap();
        let tempdir = tempfile::tempdir().unwrap();

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::new(storage, file.path(), tempdir.path())?;
        let line_str =
            "weather,location=us-midwest,country=us temperature=0,humidity=1 1465839830100400200";
        engine.ingest(&line_str)?;
        let line2_str =
            "weather,location=us-midwest,country=us temperature=2,humidity=3 1465839830100400201";
        engine.ingest(&line2_str)?;

        engine.wal.corrupt_last_record()?;

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::restore_from_wal(storage, file.path(), tempdir.path())?;

        engine.storage.make_snapshot();
        let metrics = engine.storage.load_from_snapshot("weather:temperature");

        assert_eq!(
            metrics
                .iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(f64, u64)>>(),
            vec![(0f64, 1465839830100400200)]
        );
        drop(metrics);

        let line2_str = "weather,location=us-midwest,country=us temperature=4 1465839830100400202";
        engine.ingest(&line2_str)?;
        engine.wal.flush_and_sync()?;

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::restore_from_wal(storage, file.path(), tempdir.path())?;

        engine.storage.make_snapshot();
        let metrics = engine.storage.load_from_snapshot("weather:temperature");

        assert_eq!(
            metrics
                .iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(f64, u64)>>(),
            vec![(0f64, 1465839830100400200), (4f64, 1465839830100400202)]
        );
        drop(metrics);

        engine.ingest(&line2_str)?;
        Ok(())
    }
}

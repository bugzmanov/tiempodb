use crate::partition::PartitionManager;
use crate::protocol;
use crate::storage;
use crate::storage::DataPoint;
use crate::storage::SnaphotableStorage;
use crate::storage::Storage;
use crate::wal::Wal;
use crate::wal::WalBlockReader;
use crossbeam::channel;
use crossbeam::channel::{bounded, RecvError, TryRecvError};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use streaming_iterator::StreamingIterator;

struct Engine {
    storage: SnaphotableStorage,
    metrics_cache: HashMap<String, Rc<str>>,
    wal: Wal,
    partition_manager: PartitionManager,
}

impl Engine {
    pub fn new(
        storage: SnaphotableStorage,
        wal_path: &Path,
        partitions_path: &Path,
    ) -> io::Result<Self> {
        Ok(Engine {
            storage,
            metrics_cache: HashMap::new(),
            wal: Wal::new(wal_path)?,
            partition_manager: PartitionManager::new(partitions_path)?,
        })
    }

    //todo: ingest multi-line
    pub fn ingest(&mut self, line_str: &str) -> io::Result<()> {
        self.wal.write(line_str.as_bytes())?;
        self.save_to_storage(line_str);
        // if self.storage.active_set_size() > 10 {
        //     self.storage.make_snapshot();
        //     self.partition_manager.roll_new_partition();
        // }
        Ok(())
    }

    fn save_to_storage(&mut self, line_str: &str) {
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

    pub fn restore_from_wal(
        storage: SnaphotableStorage,
        wal_path: &Path,
        partitions_path: &Path,
    ) -> io::Result<Self> {
        let mut iter = WalBlockReader::read(wal_path)?.into_iter();
        let mut storage = Engine {
            storage,
            metrics_cache: HashMap::new(),
            wal: Wal::new(wal_path)?, //todo: seek to the end
            partition_manager: PartitionManager::new(partitions_path)?,
        };
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
        dbg!(iter.last_successfull_read_position());
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
    snapshot: Arc<RwLock<HashMap<Rc<str>, Vec<DataPoint>>>>,
}

impl PartitionWorker {
    pub fn run(&mut self) {
        while self.tick() {}
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
            Err(e) => todo!("handle failure"),
        }
    }

    fn roll_partition(&mut self) -> io::Result<()> {
        let r = self.snapshot.read();
        self.partition_manager.roll_new_partition(&*r).map(|_| ()) //todo: handle failure
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_test() -> io::Result<()> {
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
        let metrics = engine.storage.load_unsafe("weather:temperature");

        assert_eq!(
            metrics
                .into_iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(i64, u64)>>(),
            vec![(0, 1465839830100400200), (2, 1465839830100400201)]
        );
        Ok(())
    }

    #[test]
    fn test_restore_from_wal() -> io::Result<()> {
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

        let metrics = engine.storage.load_unsafe("weather:temperature");

        assert_eq!(
            metrics
                .into_iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(i64, u64)>>(),
            vec![(0, 1465839830100400200), (2, 1465839830100400201)]
        );
        Ok(())
    }

    #[test]
    fn test_restore_from_corrupt_wall() -> io::Result<()> {
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

        engine.wal.corrupt_last_record()?;

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::restore_from_wal(storage, file.path(), tempdir.path())?;

        let metrics = engine.storage.load_unsafe("weather:temperature");

        assert_eq!(
            metrics
                .into_iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(i64, u64)>>(),
            vec![(0, 1465839830100400200)]
        );

        let line2_str = "weather,location=us-midwest,country=us temperature=4 1465839830100400202";
        engine.ingest(&line2_str)?;
        engine.wal.flush_and_sync()?;

        let storage = storage::SnaphotableStorage::new();
        let mut engine = Engine::restore_from_wal(storage, file.path(), tempdir.path())?;

        let metrics = engine.storage.load_unsafe("weather:temperature");

        assert_eq!(
            metrics
                .into_iter()
                .map(|m| (m.value, m.timestamp))
                .collect::<Vec<(i64, u64)>>(),
            vec![(0, 1465839830100400200), (4, 1465839830100400202)]
        );

        engine.ingest(&line2_str)?;
        Ok(())
    }
}

use crate::storage::DataPoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

#[inline]
fn ignore_not_found(result: io::Result<()>) -> io::Result<()> {
    match result {
        ok @ Ok(_) => ok,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        err @ Err(_) => err,
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Partition {
    pub start_time: u64,
    pub end_time: u64,
    pub metrics: Vec<MetricsMeta>,
}

impl Partition {
    fn new() -> Self {
        Partition {
            start_time: 0u64,
            end_time: 0u64,
            metrics: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MetricsMeta {
    pub metric_name: String,
    pub start_time: u64,
    pub end_time: u64,
    pub size: usize,
    pub start_offset: u64,
    pub end_offset: u64,
    pub uncompressed_size: u64,
    pub crc32: u32,
}

impl MetricsMeta {
    fn new(
        metric_name: String,
        start_time: u64,
        end_time: u64,
        size: usize,
        uncompressed_size: u64,
    ) -> Self {
        MetricsMeta {
            metric_name,
            start_time,
            end_time,
            size,
            uncompressed_size,
            start_offset: 0,
            end_offset: 0,
            crc32: 0,
        }
    }

    fn size_on_disk(&self) -> u64 {
        self.end_offset - self.start_offset
    }
}

struct PartitionWriter {
    // data_dir: Path,
}

impl PartitionWriter {
    pub fn write_partition(
        path: &Path,
        data: &HashMap<Rc<str>, Vec<DataPoint>>,
    ) -> io::Result<Partition> {
        let file = fs::OpenOptions::new().write(true).create(true).open(path)?;

        let zstd_level = zstd::compression_level_range()
            .last()
            .expect("At least one compression level should be provided");

        let mut buf_writer =
            io::BufWriter::new(zstd::Encoder::new(file, zstd_level).expect("zstd encoder failure"));

        let mut partition = Partition::new();
        let mut partition_start_time = 0;
        let mut partition_end_time = 0;
        for (metric_name, ref mut points) in data {
            // points.sort_by_key(|metric| metric.timestamp);
            let mut meta = MetricsMeta::new(
                metric_name.to_string(),
                points.first().unwrap().timestamp,
                points.last().unwrap().timestamp,
                points.len() as usize,
                points.len() as u64 * 16,
            );
            meta.start_offset = buf_writer.get_ref().get_ref().stream_position()?; //todo

            for point in points.iter() {
                buf_writer.write_all(&point.timestamp.to_le_bytes())?;
                buf_writer.write_all(&point.value.to_le_bytes())?;
            }
            buf_writer.flush()?;
            buf_writer.get_mut().get_mut().sync_all()?;
            meta.end_offset = buf_writer.get_ref().get_ref().stream_position()?; //todo

            partition_start_time = partition_start_time.max(meta.start_time);
            partition_end_time = partition_end_time.max(meta.end_time);
            partition.metrics.push(meta);
        }

        Ok(partition)
    }
}

struct PartitionReader {}

impl PartitionReader {
    pub fn read_partition(
        path: &Path,
        partition: &Partition,
    ) -> io::Result<HashMap<Rc<str>, Vec<DataPoint>>> {
        // data.sort_by_key(|metric| metric.timestamp);
        dbg!(path);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let mut buf_reader =
            io::BufReader::new(zstd::Decoder::new(file).expect("zstd encoder failure"));

        let first = partition.metrics.first().unwrap();
        let mut buf = vec![0; first.uncompressed_size as usize];

        let mut result = HashMap::new();
        for metric_meta in partition.metrics.iter() {
            let name: Rc<str> = Rc::from(metric_meta.metric_name.as_str());
            let mut metrics = Vec::with_capacity(metric_meta.size);
            if buf.capacity() < metric_meta.uncompressed_size as usize {
                buf = vec![0; metric_meta.uncompressed_size as usize];
            }
            dbg!(buf.capacity());

            buf_reader.read_exact(&mut buf)?;
            for point in buf.chunks(16) {
                let timestamp = u64::from_le_bytes(point[0..8].try_into().unwrap());
                let value = i64::from_le_bytes(point[8..16].try_into().unwrap());
                metrics.push(DataPoint::new(name.clone(), timestamp, value))
            }

            result.insert(name.clone(), metrics);
        }

        Ok(result)
    }
}

pub struct PartitionManager {
    pub partitions_dir: PathBuf,
    pub last_partition_id: usize,
    pub partitions: Vec<Partition>,
}

impl PartitionManager {
    pub fn new(partitions_dir: &Path) -> io::Result<Self> {
        let mut manager = PartitionManager {
            partitions_dir: partitions_dir.to_path_buf(),
            last_partition_id: 0,
            partitions: Vec::new(), //todo: unnecessary allocation
        };

        let mut existing_partitions = manager.list_partitions()?;
        if !existing_partitions.is_empty() {
            existing_partitions.sort_by_key(|kv| kv.0);
            manager.last_partition_id = existing_partitions.last().unwrap().0;
            manager.partitions = existing_partitions.into_iter().map(|kv| kv.1).collect();
        }

        Ok(manager)
    }

    fn tmp_data_file(&self, partition_id: usize) -> PathBuf {
        self.partitions_dir
            .join(format!("partition_{partition_id}.data-tmp"))
    }

    fn data_file(&self, partition_id: usize) -> PathBuf {
        self.partitions_dir
            .join(format!("partition_{partition_id}.data"))
    }

    fn meta_file(&self, partition_id: usize) -> PathBuf {
        self.partitions_dir
            .join(format!("partition_{partition_id}.meta"))
    }

    fn try_recover(&mut self, partition_id: usize) -> io::Result<bool> {
        let tmp_data_file = self.tmp_data_file(partition_id);
        let data_file = self.data_file(partition_id);
        let meta_fila = self.meta_file(partition_id);
        if !fs::try_exists(meta_fila)? {
            return Ok(false);
        }
        if fs::try_exists(&data_file)? {
            // this is weird condition, should probably never happen
            Ok(true)
        } else if fs::try_exists(&tmp_data_file)? {
            fs::rename(tmp_data_file, data_file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn remove_partition_if_exists(&self, partition_id: usize) -> io::Result<()> {
        ignore_not_found(fs::remove_file(self.tmp_data_file(partition_id)))?;
        ignore_not_found(fs::remove_file(self.data_file(partition_id)))?;
        ignore_not_found(fs::remove_file(self.meta_file(partition_id)))
    }

    fn list_partitions(&self) -> io::Result<Vec<(usize, Partition)>> {
        let mut result = Vec::new();
        for dir_entry_res in fs::read_dir(&self.partitions_dir)? {
            let dir_entry = dir_entry_res?;
            let file_name = if let Ok(file) = dir_entry.file_name().into_string() {
                file
            } else {
                continue;
            };

            if let Some((idx, ttype)) = PartitionManager::parse_file_name(&file_name) {
                if ttype != "meta" {
                    continue;
                }
                let metadata = PartitionManager::load_meta(&dir_entry.path())?;
                if fs::try_exists(self.data_file(idx))? {
                    result.push((idx, metadata));
                }
            }
        }
        Ok(result)
    }

    fn parse_file_name(file_name: &str) -> Option<(usize, &str)> {
        if let [name, suffix] = file_name.split('_').collect::<Vec<&str>>().as_slice() {
            if *name != "partition" {
                return None;
            }
            if let [idx, ttype] = (*suffix).split('.').collect::<Vec<&str>>().as_slice() {
                (*idx)
                    .parse::<usize>()
                    .map_or(None, |idx_num| Some((idx_num, *ttype)))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn roll_new_partition(
        &mut self,
        metrics: &HashMap<Rc<str>, Vec<DataPoint>>,
    ) -> io::Result<&Partition> {
        let next_partition_id = self.last_partition_id + 1;
        let tmp_partition_file = self.tmp_data_file(next_partition_id);
        let metadata_file = self.meta_file(next_partition_id);

        if fs::try_exists(tmp_partition_file.clone())? {
            if let Ok(true) = self.try_recover(next_partition_id) {
                self.partitions
                    .push(PartitionManager::load_meta(&metadata_file)?);
                self.last_partition_id = next_partition_id;
                return self.roll_new_partition(metrics);
            }
            self.remove_partition_if_exists(next_partition_id)?;
        }

        fail::fail_point!("pm-roll-write-meta-step", |_| {
            Err(io::Error::new(io::ErrorKind::TimedOut, "error"))
        });
        let new_partition = PartitionWriter::write_partition(&tmp_partition_file, metrics)?;

        PartitionManager::save_meta(&metadata_file, &new_partition)?;
        fail::fail_point!("pm-roll-rename-step", |_| {
            Err(io::Error::new(io::ErrorKind::TimedOut, "error"))
        });
        fs::rename(tmp_partition_file, self.data_file(next_partition_id))?;
        self.last_partition_id = next_partition_id;
        self.partitions.push(new_partition);

        Ok(self.partitions.last().unwrap())
    }

    fn save_meta(path: &Path, partition: &Partition) -> io::Result<()> {
        let json = serde_json::to_string(partition)?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        file.sync_all()
    }

    //todo introduce anyhow
    fn load_meta(path: &Path) -> io::Result<Partition> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let file_size = file.metadata()?.len() as usize;
        let mut reader = io::BufReader::new(file);
        let mut data = Vec::with_capacity(file_size);
        reader.read_to_end(&mut data)?;

        match String::from_utf8(data) {
            Ok(data_str) => match serde_json::from_str::<Partition>(&data_str) {
                Ok(partition) => Ok(partition),
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            },
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }
}

#[cfg(test)]
mod test {
    use claim::assert_none;
    #[cfg(feature = "fail/failpoints")]
    use fail::{fail_point, FailScenario};
    use std::collections::{HashMap, HashSet};

    use super::*;

    fn generate_metric(metric_name: &str) -> HashMap<Rc<str>, Vec<DataPoint>> {
        let mut data = HashMap::new();
        let metric_name: Rc<str> = Rc::from(metric_name);
        data.insert(
            metric_name.clone(),
            vec![DataPoint::new(metric_name.clone(), 100u64, 200i64)],
        );
        data
    }
    fn generate_metrics_batch(metric_suffix: &str) -> HashMap<Rc<str>, Vec<DataPoint>> {
        let mut data = HashMap::new();
        (0..10).for_each(|metric_idx| {
            let metric_name: Rc<str> = Rc::from(format!("metric_{metric_suffix}_{metric_idx}"));
            data.insert(
                metric_name.clone(),
                (0..10)
                    .map(|i| DataPoint::new(metric_name.clone(), 100u64 + i, 200i64 + i as i64))
                    .collect(),
            );
        });
        data
    }

    #[test]
    fn test_partition_read_write() -> io::Result<()> {
        let file = tempfile::NamedTempFile::new()?;

        let mut data = generate_metrics_batch("");
        let partition = PartitionWriter::write_partition(file.path(), &mut data)?;
        let read_data = PartitionReader::read_partition(file.path(), &partition)?;

        assert_eq!(read_data, data);

        Ok(())
    }

    #[test]
    fn test_partion_meta_write_read() -> io::Result<()> {
        let file = tempfile::NamedTempFile::new()?;

        let mut partition = Partition::new();
        partition.metrics.push(MetricsMeta::new(
            "metric1".to_string(),
            1234,
            4567,
            8910,
            1234,
        ));
        partition
            .metrics
            .push(MetricsMeta::new("metric2".to_string(), 14, 47, 810, 1321));
        partition.start_time = 10;
        partition.end_time = 60;

        PartitionManager::save_meta(&file.path(), &partition)?;

        let read_partition = PartitionManager::load_meta(&file.path())?;

        assert_eq!(read_partition, partition);

        Ok(())
    }

    #[test]
    fn parse_file_name_success() {
        let (idx, ttyp) = PartitionManager::parse_file_name("partition_12.meta").unwrap();
        assert_eq!(12, idx);
        assert_eq!("meta", ttyp);
    }

    #[test]
    fn parse_file_name_bad_format() {
        assert_none!(PartitionManager::parse_file_name("partition_12")); // no type
        assert_none!(PartitionManager::parse_file_name("partition.meta")); // no idx
        assert_none!(PartitionManager::parse_file_name(
            "partition_notanumber.meta"
        )); // idx is no number
        assert_none!(PartitionManager::parse_file_name(
            "partition_12_this_should_not_exist.meta"
        )); // additional suffixes
    }

    #[test]
    #[cfg(not(feature = "fail/failpoints"))]
    fn test_roll_new_partition() -> io::Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let mut manager = PartitionManager::new(&tempdir.path())?;
        let mut metrics = generate_metrics_batch("first");

        let original_metrics: HashSet<String> = metrics.keys().map(|k| k.to_string()).collect();
        manager.roll_new_partition(&mut metrics)?;

        let mut manager = PartitionManager::new(&tempdir.path())?;
        assert_eq!(manager.partitions.len(), 1);

        let loaded_metrics: HashSet<String> = manager
            .partitions
            .get(0)
            .unwrap()
            .metrics
            .iter()
            .map(|m| m.metric_name.clone())
            .collect();

        assert_eq!(loaded_metrics, original_metrics);

        let mut metrics = generate_metrics_batch("second");
        manager.roll_new_partition(&mut metrics)?;

        let second_metrics: HashSet<String> = metrics.keys().map(|k| k.to_string()).collect();

        let manager = PartitionManager::new(&tempdir.path())?;
        assert_eq!(manager.partitions.len(), 2);

        let loaded_metrics: HashSet<String> = manager
            .partitions
            .get(1)
            .unwrap()
            .metrics
            .iter()
            .map(|m| m.metric_name.clone())
            .collect();

        assert_eq!(loaded_metrics, second_metrics);

        Ok(())
    }

    #[test]
    #[cfg(feature = "fail/failpoints")]
    fn test_recoverable_partition_failure() -> io::Result<()> {
        let scenario = FailScenario::setup();
        fail::cfg("pm-roll-rename-step", "return").unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let mut manager = PartitionManager::new(&tempdir.path())?;
        let mut metrics = generate_metric("first");
        assert_eq!(true, manager.roll_new_partition(&mut metrics).is_err());

        fail::cfg("pm-roll-rename-step", "off").unwrap();

        let mut metrics = generate_metric("second");
        manager.roll_new_partition(&mut metrics)?;

        assert_eq!(2, manager.partitions.len());

        let metrics: HashSet<String> = manager
            .partitions
            .iter()
            .flat_map(|p| p.metrics.iter().map(|m| m.metric_name.clone()))
            .collect();

        assert_eq!(
            metrics,
            vec!["first".to_string(), "second".to_string()]
                .into_iter()
                .collect()
        );

        scenario.teardown();
        Ok(())
    }

    #[test]
    #[cfg(feature = "fail/failpoints")]
    fn test_partition_failure_data_loss() -> io::Result<()> {
        let scenario = FailScenario::setup();
        fail::cfg("pm-roll-write-meta-step", "return").unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let mut manager = PartitionManager::new(&tempdir.path())?;
        let mut metrics = generate_metric("first");
        assert_eq!(true, manager.roll_new_partition(&mut metrics).is_err());

        fail::cfg("pm-roll-write-meta-step", "off").unwrap();

        let mut metrics = generate_metric("second");
        manager.roll_new_partition(&mut metrics)?;

        let metrics: HashSet<String> = manager
            .partitions
            .iter()
            .flat_map(|p| p.metrics.iter().map(|m| m.metric_name.clone()))
            .collect();

        assert_eq!(metrics, vec!["second".to_string()].into_iter().collect());
        scenario.teardown();
        Ok(())
    }
}

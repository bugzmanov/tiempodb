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

// macro_rules! ignore_not_found {
//     ($a:ident($($b:tt)*))=>{
//        {
//         match $a($($b)*) {
//             ok@Ok => ok,
//             Err(err) if err == io::ErrorKind::NotFound => Ok(())
//             err@Err => err
//         }
//         }
//     };
// }

#[inline]
fn ignore_not_found(result: io::Result<()>) -> io::Result<()> {
    return match result {
        ok @ Ok(_) => ok,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        err @ Err(_) => err,
    };
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Partition {
    start_time: u64,
    end_time: u64,
    metrics: Vec<MetricsMeta>,
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
struct MetricsMeta {
    metric_name: String,
    start_time: u64,
    end_time: u64,
    size: usize,
    start_offset: u64,
    end_offset: u64,
    uncompressed_size: u64,
    crc32: u32,
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
        data: &mut HashMap<Rc<str>, Vec<DataPoint>>, //todo: get rid of mut
    ) -> io::Result<Partition> {
        // data.sort_by_key(|metric| metric.timestamp);
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
            points.sort_by_key(|metric| metric.timestamp);
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

struct PartitionManager {
    partitions_dir: PathBuf,
    last_partition_id: usize,
}

impl PartitionManager {
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

    fn try_recover(&mut self, parition_id: usize) -> io::Result<bool> {
        todo!("not implemented yet")
    }

    fn remove_partition(&self, partition_id: usize) -> io::Result<()> {
        ignore_not_found(fs::remove_file(self.tmp_data_file(partition_id)))?;
        ignore_not_found(fs::remove_file(self.meta_file(partition_id)))
    }

    fn list_partitions(&self) -> io::Result<Vec<Partition>> {
        let mut result = Vec::new();
        for dir_entry_res in fs::read_dir(self.partitions_dir.clone())? {
            let dir_entry = dir_entry_res?;
            let file_name = if let Ok(file) = dir_entry.file_name().into_string() {
                file
            } else {
                continue;
            };

            if let Some((size, ttype)) = PartitionManager::parse_file_name(&file_name) {
                //todo
                todo!("sasda")
            }
        }
        Ok(result)
    }

    fn parse_file_name(file_name: &str) -> Option<(usize, &str)> {
        if let [name, suffix] = file_name.split("_").collect::<Vec<&str>>().as_slice() {
            if *name != "partition" {
                return None;
            }
            if let [idx, ttype] = (*suffix).split(".").collect::<Vec<&str>>().as_slice() {
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

    fn roll_new_partition(
        &mut self,
        metrics: &mut HashMap<Rc<str>, Vec<DataPoint>>,
    ) -> io::Result<()> {
        let next_partition_id = self.last_partition_id + 1;
        let tmp_partition_file = self.tmp_data_file(next_partition_id);

        if fs::try_exists(tmp_partition_file.clone())? {
            if let Ok(true) = self.try_recover(next_partition_id) {
                return self.roll_new_partition(metrics);
            }
            self.remove_partition(next_partition_id)?;
        }

        let new_partition = PartitionWriter::write_partition(&tmp_partition_file, metrics)?; //todo: clean ups in case of failure

        let metadata_file = self.meta_file(next_partition_id);
        PartitionManager::save_meta(&metadata_file, &new_partition)?;
        fs::rename(tmp_partition_file, self.data_file(next_partition_id))?;
        self.last_partition_id = next_partition_id;

        Ok(())
    }

    fn save_meta(path: &Path, partition: &Partition) -> io::Result<()> {
        let json = serde_json::to_string(partition)?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        file.flush()
    }

    //todo introduce anyhow
    fn load_meta(path: &Path) -> io::Result<Partition> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let file_size = file.metadata()?.len() as usize;
        let mut reader = io::BufReader::new(file);
        let mut data = Vec::with_capacity(file_size);
        reader.read_to_end(&mut data)?;
        // String::from_utf8(data).map(|json_str| serde_json::from_str::<Partition>(&json_str))
        match String::from_utf8(data) {
            Ok(data_str) => match serde_json::from_str::<Partition>(&data_str) {
                Ok(partition) => return Ok(partition),
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to parse json in {:?}", path.to_str()),
                    ))
                }
            },
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to parse json in {:?}", path.to_str()),
                ))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use claim::assert_none;

    use super::*;

    #[test]
    fn test_partition_read_write() -> io::Result<()> {
        let file = tempfile::NamedTempFile::new()?;

        let mut data = HashMap::new();
        (0..10).for_each(|metric_idx| {
            let metric_name: Rc<str> = Rc::from(format!("metric_{metric_idx}"));
            data.insert(
                metric_name.clone(),
                (0..10)
                    .map(|i| DataPoint::new(metric_name.clone(), 100u64 + i, 200i64 + i as i64))
                    .collect(),
            );
        });
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
        )); // idx is no number
    }
}

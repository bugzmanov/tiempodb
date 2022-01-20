use crate::storage::DataPoint;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::rc::Rc;

#[derive(Debug)]
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

#[derive(Debug)]
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
        data: &mut HashMap<Rc<str>, Vec<DataPoint>>,
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
        for ref metric_meta in partition.metrics.iter() {
            let name: Rc<str> = Rc::from(metric_meta.metric_name.as_str());
            let mut metrics = Vec::with_capacity(metric_meta.size);
            if buf.capacity() < metric_meta.uncompressed_size as usize {
                buf = vec![0; metric_meta.uncompressed_size as usize];
            }
            dbg!(buf.capacity());

            buf_reader.read_exact(&mut buf)?;
            // buf_reader = buf_reader.take(metric_meta.uncompressed_size).
            // buf_reader
            //     .read_to_end(&mut buf);
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ial() -> io::Result<()> {
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
}

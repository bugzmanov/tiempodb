use std::io::{self, BufReader, Read, Seek};
use std::{fs, io::Write};
use streaming_iterator::StreamingIterator;

pub struct Wal {
    log: fs::File,
}

impl Wal {
    pub fn new(path: &str) -> io::Result<Self> {
        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Wal { log })
    }

    fn crc32(block: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(block);
        hasher.finalize()
    }

    pub fn flush_and_sync(&mut self) -> io::Result<()> {
        self.log.flush()?;
        self.log.sync_all()?;
        Ok(())
    }

    pub fn write(&mut self, block: &[u8]) -> io::Result<()> {
        let crc32 = Wal::crc32(block);
        self.log.write_all(&(block.len() as u64).to_le_bytes())?;
        self.log.write_all(&crc32.to_le_bytes())?;
        self.log.write_all(block)?;
        Ok(())
    }

    #[cfg(test)]
    fn corrupt_last_record(&mut self) -> io::Result<()> {
        self.log.seek(io::SeekFrom::End(-3))?;
        self.log.set_len(self.log.metadata()?.len() - 3)?;
        self.log.flush()?;
        self.log.sync_all()?;
        Ok(())
    }
}

pub struct WalBlockReader {
    reader: BufReader<fs::File>,
    buf: Vec<u8>,
    header_buf: [u8; 8 + 4],
    file_name: String,
}

impl WalBlockReader {
    pub fn read(path: &str) -> io::Result<WalBlockReader> {
        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let reader = BufReader::new(log);

        let header_buf = [u8::default(); 8 + 4];
        let block_max_size = 10 * 1024 * 1024 * 1024; //10 MiB
        let buf = vec![0; block_max_size];
        io::Result::Ok(WalBlockReader {
            reader,
            buf,
            header_buf,
            file_name: path.into(),
        })
    }

    pub fn into_iter(self) -> WalBlockIterator {
        WalBlockIterator {
            link: self,
            status: Ok(None),
        }
    }

    fn file_size(&self) -> io::Result<u64> {
        Ok(self.reader.get_ref().metadata()?.len())
    }
}

pub struct WalBlockIterator {
    link: WalBlockReader,
    status: Result<Option<usize>, io::Error>,
}

impl WalBlockIterator {
    pub fn consume_next<F: FnOnce(Result<&[u8], io::Error>)>(&mut self, consumer: F) -> bool {
        let mut status = Ok(None);
        let result = self._consume_next(|block| {
            match &block {
                Ok(data) => status = Ok(Some(data.len())),
                Err(e) => status = Err(io::Error::new(e.kind(), e.to_string())),
            }
            consumer(block);
        });
        self.status = status;
        result
    }

    fn _consume_next<F: FnOnce(Result<&[u8], io::Error>)>(&mut self, consumer: F) -> bool {
        if let Err(ref e) = self.status {
            consumer(Err(io::Error::new(e.kind(), e.to_string())));
            return false;
        }
        if let Err(t) = self.link.reader.read_exact(&mut self.link.header_buf) {
            if t.kind() != std::io::ErrorKind::UnexpectedEof {
                consumer(Err(t));
            }
            return false;
        }

        let block_size = usize::from_le_bytes(self.link.header_buf[0..8].try_into().unwrap());
        let expected_crc32 = u32::from_le_bytes(self.link.header_buf[8..].try_into().unwrap());
        match self.link.file_size() {
            Err(e) => {
                consumer(Err(io::Error::new(e.kind(), e.to_string())));
                return false;
            }
            Ok(file_size) if (file_size as usize) < block_size => {
                consumer(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: block_size is corrupted", self.link.file_name),
                )));
                return false;
            }
            _ => { /* do nothing */ }
        }

        if block_size >= self.link.buf.capacity() {
            self.link.buf = Vec::with_capacity(block_size); //todo extend?
        }

        if let Err(e) = self
            .link
            .reader
            .read_exact(&mut self.link.buf[0..block_size])
        {
            consumer(Err(e));
            return false;
        }
        let block_crc32 = Wal::crc32(&self.link.buf[0..block_size]);
        if block_crc32 != expected_crc32 {
            consumer(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}: crc32 error check fail.Data block on a disk is broken",
                    self.link.file_name
                ),
            )));
            return false;
        }
        consumer(Ok(&self.link.buf[0..block_size]));
        true
    }
}

impl StreamingIterator for WalBlockIterator {
    type Item = [u8];

    fn advance(&mut self) {
        self.consume_next(|_| {});
    }

    fn get(&self) -> Option<&[u8]> {
        if let Ok(Some(size)) = self.status {
            Some(&self.link.buf[0..size])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use claim::*;

    #[test]
    fn basic() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let file_name = file.path().to_str().unwrap();
        let mut wal = Wal::new(file_name).unwrap();
        wal.write("vo pole bereza stoyala".as_bytes()).unwrap();
        wal.write("vo pole kudryavaya stoyala".as_bytes()).unwrap();
        wal.flush_and_sync().unwrap();

        let reader = WalBlockReader::read(file_name).unwrap();
        let mut iter = reader.into_iter();
        let mut result = Vec::new();

        while false
            != iter.consume_next(|block| match block {
                Ok(buf) => unsafe { result.push(String::from_utf8_unchecked(Vec::from(buf))) }, //result.push(Vec::from(buf)),
                Err(r) => panic!("{}", r),
            })
        {}

        assert_eq!(
            result,
            vec![
                "vo pole bereza stoyala".to_string(),
                "vo pole kudryavaya stoyala".to_string()
            ]
        );
    }

    #[test]
    fn streaming_iterator() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let file_name = file.path().to_str().unwrap();
        let mut wal = Wal::new(file_name).unwrap();
        wal.write("vo pole bereza stoyala".as_bytes()).unwrap();
        wal.write("vo pole kudryavaya stoyala".as_bytes()).unwrap();
        wal.flush_and_sync().unwrap();

        let reader = WalBlockReader::read(file_name).unwrap();
        let mut iter = reader.into_iter();
        let mut result = Vec::new();
        loop {
            iter.advance();
            match iter.get() {
                Some(v) => {
                    unsafe { result.push(String::from_utf8_unchecked(Vec::from(v))) };
                }
                None => break,
            }
        }

        assert_eq!(
            result,
            vec![
                "vo pole bereza stoyala".to_string(),
                "vo pole kudryavaya stoyala".to_string()
            ]
        );
    }

    #[test]
    fn corrupt_wal_log() -> Result<(), io::Error> {
        let file = tempfile::NamedTempFile::new()?;
        let file_name = file.path().to_str().unwrap();

        let mut wal = Wal::new(file_name)?;
        wal.write("vo pole bereza stoyala".as_bytes())?;
        wal.write("vo pole kudryavaya stoyala".as_bytes())?;
        wal.flush_and_sync()?;

        wal.corrupt_last_record()?;

        wal.write("small".as_bytes())?;
        wal.flush_and_sync()?;

        let reader = WalBlockReader::read(file_name).unwrap();
        let mut iter = reader.into_iter();
        let mut result = Vec::new();
        loop {
            iter.advance();
            match iter.get() {
                Some(v) => {
                    unsafe { result.push(String::from_utf8_unchecked(Vec::from(v))) };
                }
                None => break,
            }
        }
        assert_eq!(result, vec!["vo pole bereza stoyala".to_string(),]);

        for _ in 0..10 {
            iter.advance();
            assert_none!(iter.get());
        }
        Ok(())
    }
}

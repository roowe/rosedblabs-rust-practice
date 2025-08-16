use crate::log::Log;
use crate::log::KeyDir;
use std::path::PathBuf;
use anyhow::Result;

pub struct MiniBitcask {
    log: Log,
    index: KeyDir, // key -> (value_pos, value_len)
}

impl Drop for MiniBitcask {
    fn drop(&mut self) {
        println!("drop bitcask");
        if let Err(e) = self.flush() {
            eprintln!("error flushing bitcask: {}", e);
        }
    }
}

impl MiniBitcask {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let index = log.load_index()?;
        Ok(Self { log, index })
    }
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let (offset, len) = self.log.write_entry(key, Some(&value))?;
        let value_len = value.len() as u32;
        self.index.insert(key.to_vec(), (offset+len as u64 -value_len as u64, value_len));
        Ok(())
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.index.get(key) {
            Some((offset, len)) => {
                let value = self.log.read_value(*offset, *len)?;
                Ok(Some(value))
            }
            None => {
                Ok(None)
            }
        }
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.index.remove(key);
        Ok(())
    }
    
    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_bitcask() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new_in(".")?;
        let tmp_path = tmp_dir.path().join("test.db");
        let mut bitcask = MiniBitcask::new(tmp_path)?;
        bitcask.set(b"a", b"val1")?;
        bitcask.set(b"b", b"val2")?;
        bitcask.set(b"c", b"val3")?;
        Ok(())
    }
}
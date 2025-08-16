use anyhow::Result;
use fs4::fs_std::FileExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
const KEY_VAL_HEADER_LEN: u32 = 4;

type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

#[derive(Debug)]
struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.try_lock_exclusive()?;
        Ok(Self { path, file })
    }

    fn read_value(&mut self, valus_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(valus_pos))?;
        let mut value = vec![0; value_len as usize];
        self.file.read_exact(&mut value)?;
        Ok(value)
    }
    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tomestone = value.map_or(-1, |v| v.len() as i32);
        let len = KEY_VAL_HEADER_LEN * 2 + key_len + value_len;

        let offset = self.file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tomestone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }
        w.flush()?;
        println!("write_entry: key: {:?}, value: {:?}", key, value);
        println!("offset: {}, len: {}", offset, len);
        println!("key_len: {}, value_len_or_tomestone: {:?}", key_len, value_len_or_tomestone);

        Ok((offset, len))
    }

    fn load_index(&mut self) -> Result<KeyDir> {
        let mut index = KeyDir::new();
        let mut len_buf = [0; 4];

        let file_size = self.file.metadata()?.len();
        let mut r = BufReader::with_capacity(1024, &mut self.file);
        let mut pos: u64 = r.seek(SeekFrom::Start(0))?;

        while pos < file_size {
            let read_one = || -> Result<(Vec<u8>, u64, Option<u32>)> {
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let value_len_or_tomestone = match i32::from_be_bytes(len_buf) {
                    v if v >= 0 => Some(v as u32),
                    _ => None,
                };

                let value_pos: u64 = pos + (KEY_VAL_HEADER_LEN * 2 + key_len) as u64;

                println!("pos: {}, key_len: {}, value_len_or_tomestone: {:?}", pos, key_len, value_len_or_tomestone);

                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key)?;
                if let Some(value_len) = value_len_or_tomestone {
                    r.seek_relative(value_len as i64)?;
                }
                Ok((key, value_pos, value_len_or_tomestone))
            }();
            match read_one {
                Ok((key, value_pos, Some(value_len))) => {
                    index.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    index.remove(&key);
                    pos = value_pos;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(index)
    }
}

// #[cfg(unix)]
// fn read_at_position(file: &std::fs::File, buffer: &mut [u8], pos: u64) -> std::io::Result<usize> {
//     use std::os::unix::fs::FileExt;
//     file.read_at(buffer, pos)  // Unix 使用 read_at
// }

// #[cfg(windows)]
// fn read_at_position(file: &std::fs::File, buffer: &mut [u8], pos: u64) -> std::io::Result<usize> {
//     use std::os::windows::fs::FileExt;
//     file.seek_read(buffer, pos)  // Windows 使用 seek_read
// }

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_log() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new_in(".")?;
        let tmp_path = tmp_dir.path().join("test.db");

        //let tmp_path = tempfile::TempDir::new_in(".").map(|dir| dir.path().join("test.db"))?;
        // 闭包返回的是 PathBuf，TempDir 在 map 结束时就被 Drop 了，此时临时目录已被删除。
        // 随后你把这个 PathBuf 传给 Log::new，而 Log::new 里会 create_dir_all(parent)，于是又把同一路径重新创建成普通目录。这个目录已经不再受 TempDir 的清理管理，所以“不会自动删除”。
        
        let mut log = Log::new(tmp_path)?;
        log.write_entry(b"a", Some(b"val1"))?;
        log.write_entry(b"b", Some(b"val2"))?;
        log.write_entry(b"c", Some(b"val3"))?;

        // rewrite
        log.write_entry(b"a", Some(b"val5"))?;
        // delete
        log.write_entry(b"c", None)?;

        let key_dir = log.load_index()?;
        assert_eq!(key_dir.len(), 2);
        let mut keys = key_dir.keys().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, &[b"a", b"b"]);


        Ok(())
    }
}
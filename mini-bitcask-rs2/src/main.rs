use std::fs::{self, File};
use std::path::Path;
use std::mem;
use std::collections::HashMap;
use std::path::PathBuf;
// 跨平台代码可以这样写
#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use anyhow::Result;

struct MiniBitCask {
    indexes: HashMap<String, u64>,
    db_file: DBFile,
    dir_path: PathBuf,
}

impl MiniBitCask {
    fn new<P: AsRef<Path>>(dir_path: P) -> Result<MiniBitCask> {
        let dir_path = dir_path.as_ref();

        if !dir_path.exists() {
            println!("dir_path not exists, create it");
            fs::create_dir_all(dir_path)?;
        }
        println!("dir_path: {:?}", dir_path);
        let dir_path = fs::canonicalize(dir_path)?;
        println!("dir_path: {:?}", dir_path);

        let db_file = DBFile::new(&dir_path)?;
        let mut db = Self {
            db_file,
            indexes: HashMap::new(),
            dir_path: dir_path,
        };
        db.load_indexes_from_file()?;
        Ok(db)
    }
    fn load_indexes_from_file(&mut self) -> Result<()> {
        let mut offset = 0;
        loop {
            match self.db_file.read(offset) {
                Ok(entry) => {
                    let entry_size = entry.get_size();
                    match entry.mark {
                        Mark::PUT => {
                            self.indexes.insert(entry.key, offset);
                        }
                        Mark::DELETE => {
                            self.indexes.remove(&entry.key);
                        }
                    }
                    offset += entry_size as u64;
                }
                Err(e) => {
                    //println!("error: {:?}", e);
                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        {
            let entry = Entry::new(key.to_string(), value.to_vec(), Mark::PUT);
            let offset = self.db_file.offset;
            self.db_file.write(&entry)?;
            self.indexes.insert(key.to_string(), offset);
        }
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let offset = self.indexes.get(key);
        if let Some(offset) = offset {
            let entry = self.db_file.read(*offset)?;
            Ok(Some(entry.value))
        } else {
            Ok(None)
        }
    }
    fn delete(&mut self, key: &str) -> Result<()> {
        let entry = Entry::new(key.to_string(), Vec::new(), Mark::DELETE);
        self.db_file.write(&entry)?;
        self.indexes.remove(key);
        Ok(())
    }
    fn merge(&mut self) -> Result<()> {
        if self.db_file.offset == 0 {
            return Ok(());
        }
        let mut offset = 0;
        let mut valid_entries: Vec<Entry> = Vec::new();
        println!("indexes: {:?}", self.indexes);
        loop {
            match self.db_file.read(offset) {
                Ok(entry) => {
                    let entry_size = entry.get_size();
                    if let Some(entry_offset) = self.indexes.get(&entry.key) && offset == *entry_offset {
                        valid_entries.push(entry);
                    } 
                    offset += entry_size as u64;
                }
                Err(e) => {
                    println!("error: {:?}", e);
                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(e.into());
                }
            }
        }
        println!("valid_entries: {:?}", valid_entries);
        let mut merge_db_file = DBFile::new_merge(&self.dir_path)?;
        {
            for entry in valid_entries {
                let write_offset= merge_db_file.offset;
                merge_db_file.write(&entry)?;
                self.indexes.insert(entry.key, write_offset);
            }
            let old_path = self.db_file.filename.clone();
            let merge_path = merge_db_file.filename.clone();

            let tmp_dir = tempfile::TempDir::new_in(&self.dir_path)?;

            drop(mem::replace(&mut self.db_file, DBFile::new(&tmp_dir)?));
            drop(merge_db_file);

            let _ = fs::remove_file(&old_path);
            let _ = fs::rename(&merge_path, &old_path);

            self.db_file = DBFile::new(&self.dir_path)?;
        }
        Ok(())
    }

}
const FILE_NAME: &str = "minibitcask.data";
const MERGE_FILE_NAME: &str = "minibitcask.data.merge";

struct DBFile {
    file: File,
    offset: u64,
    filename: PathBuf, // 保存文件路径
}

impl DBFile {
    fn new<P: AsRef<Path>>(dir_path: P) -> Result<DBFile> {
        let filepath = dir_path.as_ref().join(FILE_NAME); // 转换为 &Path，然后调用 join
        // 在函数内部：
        // 如果 P = &str，需要 as_ref() 转换为 &Path
        // 如果 P = String，需要 as_ref() 转换为 &Path
        // 如果 P = PathBuf，需要 as_ref() 转换为 &Path
        // 如果 P = &Path，as_ref() 返回自身
        println!("filepath: {:?}", filepath);
        DBFile::new_internal(filepath)
    }
    fn new_merge<P: AsRef<Path>>(dir_path: P) -> Result<DBFile> {
        let filepath = dir_path.as_ref().join(MERGE_FILE_NAME);
        DBFile::new_internal(filepath)
    }
    fn new_internal<P: AsRef<Path>>(filepath: P) -> Result<DBFile> {
        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(filepath.as_ref())?;
        let metadata = file.metadata()?;
        println!("metadata: {:?}", metadata);
        let offset = metadata.len();
        Ok(DBFile { file, offset, filename: filepath.as_ref().to_path_buf() })
    }
    fn read_u32(&self, offset: u64) -> Result<u32> {
        let mut buffer: [u8; 4] = [0; 4];
        read_exact_at(&self.file, &mut buffer, offset)?;
        Ok(u32::from_be_bytes(buffer))
    }
    fn read_u16(&self, offset: u64) -> Result<u16> {
        let mut buffer: [u8; 2] = [0; 2];
        read_exact_at(&self.file, &mut buffer, offset)?;
        Ok(u16::from_be_bytes(buffer))
    }
    fn read_string(&self, offset: u64, size: u32) -> Result<String> {
        let mut buffer: Vec<u8> = vec![0; size as usize];
        read_exact_at(&self.file, &mut buffer, offset)?;
        Ok(String::from_utf8(buffer)?)
    }
    fn read_bytes(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let mut buffer: Vec<u8> = vec![0; size as usize];
        read_exact_at(&self.file, &mut buffer, offset)?;
        Ok(buffer)
    }
    fn read(&self, offset: u64) -> Result<Entry> {
        println!("read offset: {:?}", offset);
        let key_size = self.read_u32(offset)?;
        //println!("key_size: {:?}", key_size);
        let value_size = self.read_u32(offset + 4)?;
        let mark = self.read_u16(offset + 8)?;

        let key = if key_size > 0 {
            self.read_string(offset + 10, key_size)?
        } else {
            String::new()
        };
        let value = if value_size > 0 {
            self.read_bytes(offset + 10 + key_size as u64, value_size)?
        } else {
            Vec::new()
        };
        println!("key: {:?}, value: {:?}, mark: {:?}", key, value, mark);

        Ok(Entry::new(key, value, mark.into()))
    }
    fn write(&mut self, entry: &Entry) -> Result<()> {
        let data = entry.encode();
        write_all_at(&self.file, &data, self.offset)?;
        self.offset += data.len() as u64;
        Ok(())
    }    
}
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"));
        }
        offset += n as u64;
        buf = &mut buf[n..];
    }
    Ok(())
}

fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "short write"));
        }
        offset += n as u64;
        buf = &buf[n..];
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Mark {
    PUT = 0,
    DELETE = 1,
}
impl From<u16> for Mark {
    fn from(value: u16) -> Self {
        match value {
            0 => Mark::PUT,
            1 => Mark::DELETE,
            _ => panic!("invalid mark value: {:?}", value),
        }
    }
}
impl From<[u8; 2]> for Mark {
    fn from(bytes: [u8; 2]) -> Self {
        let value = u16::from_be_bytes(bytes);
        Mark::from(value)
    }
}

// impl Mark {
//     fn from_be_bytes(bytes: [u8; 2]) -> Self {
//         Self::from(bytes)
//     }
// }

#[derive(Debug, Clone)]
struct Entry {
    key: String,
    value: Vec<u8>,
    key_size: u32, // 平台无关
    value_size: u32,
    mark: Mark,
}
const ENTRY_HEADER_SIZE: usize = 10;
impl Entry {
    fn new(key: String, value: Vec<u8>, mark: Mark) -> Entry {
        Entry {
            key_size: key.len() as u32,
            value_size: value.len() as u32,
            mark,
            key,
            value,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.get_size());
        buffer.extend_from_slice(&self.key_size.to_be_bytes());
        buffer.extend_from_slice(&self.value_size.to_be_bytes());
        buffer.extend_from_slice(&(self.mark as u16).to_be_bytes());
        buffer.extend_from_slice(&self.key.as_bytes());
        buffer.extend_from_slice(&self.value);
        buffer
    }

    fn get_size(&self) -> usize {
        // u32 ~4.29 GB
        // u64 ~18.4 EB, 1EB = 1024 PB, 1PB = 1024 TB, 1TB = 1024 GB
        (self.key_size + self.value_size) as usize + ENTRY_HEADER_SIZE
    }
}
fn main() -> Result<()> {
    println!("Hello, world!");
    let mut db = MiniBitCask::new("test_db")?;

    db.put("key1", String::from("value1").as_bytes())?;
    db.put("key2", String::from("value2").as_bytes())?;
    db.put("key3", String::from("value3").as_bytes())?;
    db.put("key4", String::from("value4").as_bytes())?;
    db.put("key5", String::from("value5").as_bytes())?;

    let value = db.get("key1")?;
    match value {
        Some(value) => {
            println!("value: {:?}", String::from_utf8(value)?);
        }
        None => {
            println!("key1 not found");
        }
    }

    db.put("key1", String::from("value11").as_bytes())?;
    let value = db.get("key1")?;
    match value {
        Some(value) => {
            println!("value: {:?}", String::from_utf8(value)?);
        }
        None => {
            println!("key1 not found");
        }
    }

    db.delete("key1")?;
    let value = db.get("key1")?;
    match value {
        Some(value) => {
            println!("value: {:?}", String::from_utf8(value)?);
        }
        None => {
            println!("key1 not found");
        }
    }
    db.merge()?;
    // 不需要关闭文件，因为文件会自动关闭
    Ok(())
}

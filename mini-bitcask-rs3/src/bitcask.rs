use crate::log::Log;
use crate::log::KeyDir;
use std::path::PathBuf;
use anyhow::Result;
use std::collections::btree_map;
use std::ops::Bound;
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
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (offset, len) = self.log.write_entry(key, Some(&value))?;
        let value_len = value.len() as u32;
        println!("[set] offset: {}, value_len: {}", offset, value_len);
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

    pub fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> ScanIter<'_> {
        ScanIter {
            inner: self.index.range(range),
            log: &mut self.log,
        }
    }

}

pub struct ScanIter<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> ScanIter<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (offset, len)) = item;
        let value = self.log.read_value(*offset, *len)?;
        Ok((key.clone(), value))
    }
}

impl<'a> Iterator for ScanIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for ScanIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn test_bitcask() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new_in(".")?;
        let tmp_path = tmp_dir.path().join("test.db");
        let mut bitcask = MiniBitcask::new(tmp_path)?;
        bitcask.set(b"a", vec![1, 2, 3, 4])?;
        bitcask.set(b"b", vec![5, 6, 7, 8])?;
        bitcask.set(b"c", vec![9, 10, 11, 12])?;
        Ok(())
    }

    #[test]
    fn test_concurrent_writes() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new_in(".")?;
        let tmp_path = tmp_dir.path().join("test.db");

        let db = Arc::new(Mutex::new(MiniBitcask::new(tmp_path)?));

        let num_threads = 8;
        let keys_per_thread = 10;

        let mut handles = Vec::new();
        for t in 0..num_threads {
            let db_cloned = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..keys_per_thread {
                    let key = format!("k{}-{}", t, i);
                    let value = format!("v{}-{}", t, i);
                    let mut guard = db_cloned.lock().expect("lock poisoned");
                    println!("t: {}", t);
                    guard.set(key.as_bytes(), value.into_bytes()).expect("set failed");
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        // verify
        {
            let mut guard = db.lock().expect("lock poisoned");
            for t in 0..num_threads {
                for i in 0..keys_per_thread {
                    let key = format!("k{}-{}", t, i);
                    let expected = format!("v{}-{}", t, i).into_bytes();
                    let value = guard.get(key.as_bytes())?.expect("missing value");
                    assert_eq!(value, expected);
                }
            }
        }

        Ok(())
    }
    //#[test]
    // fn test_same_file() -> Result<()> {
        //如果你更想用线程来复现锁失败：不行。
        // 文件锁在同一进程内通常是“同一文件描述符”层面可重入，且你当前 Log::new 对每个实例都在同一进程中打开，不能准确验证跨进程独占。
        // 用独立二进制最可靠。
    //     let tmp_dir = tempfile::TempDir::new_in(".")?;
    //     let tmp_path = tmp_dir.path().join("test.db");
    //     let dummy_path = tmp_path.clone();
    //     let dummy_path2 = dummy_path.clone();
        
    //     let h1 = move || -> Result<()> {
    //         let mut bitcask = MiniBitcask::new(tmp_path)?;
    //         bitcask.set(b"a", b"val1")?;
    //         bitcask.set(b"b", b"val2")?;
    //         bitcask.set(b"c", b"val3")?;
    //         bitcask.flush()?;
    //         Ok(())
    //     };

    //     let h2 = move || -> Result<()> {
    //         let mut  bitcask = MiniBitcask::new(dummy_path)?;
    //         bitcask.set(b"a", b"val3")?;
    //         bitcask.set(b"b", b"val2")?;
    //         bitcask.set(b"c", b"val1")?;
    //         Ok(())
    //     };

        
    //     let h1_handle = thread::spawn(h1);
    //     let h2_handle = thread::spawn(h2);
        
    //     let r1 = h1_handle.join().unwrap();
    //     let r2 = h2_handle.join().unwrap();
    //     assert!(r1.is_ok() ^ r2.is_ok(), "应只有一个实例能持有独占锁");

       
    //     let mut bitcask = MiniBitcask::new(dummy_path2)?;
    //     assert_eq!(bitcask.get(b"a")?, Some(b"val1".to_vec()));
    //     assert_eq!(bitcask.get(b"b")?, Some(b"val2".to_vec()));
    //     assert_eq!(bitcask.get(b"c")?, Some(b"val3".to_vec()));
    //     Ok(())
    // }

    // 测试点读的情况
    #[test]
    fn test_point_opt() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new_in(".")?;
        let path = tmp_dir.path().join("test.db");
        let mut eng = MiniBitcask::new(path.clone())?;

        // 测试获取一个不存在的 key
        assert_eq!(eng.get(b"not exist")?, None);

        // 获取一个存在的 key
        eng.set(b"aa", vec![1, 2, 3, 4])?;
        assert_eq!(eng.get(b"aa")?, Some(vec![1, 2, 3, 4]));

        // 重复 put，将会覆盖前一个值
        eng.set(b"aa", vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"aa")?, Some(vec![5, 6, 7, 8]));

        // 删除之后再读取
        eng.delete(b"aa")?;
        assert_eq!(eng.get(b"aa")?, None);

        // key、value 为空的情况
        assert_eq!(eng.get(b"")?, None);
        eng.set(b"", vec![])?;
        assert_eq!(eng.get(b"")?, Some(vec![]));

        eng.set(b"cc", vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"cc")?, Some(vec![5, 6, 7, 8]));

        Ok(())
    }

    // 测试扫描
    #[test]
    fn test_scan() -> Result<()> {
        let path = std::env::temp_dir()
            .join("minibitcask-scan-test")
            .join("log");
        let mut eng = MiniBitcask::new(path.clone())?;

        eng.set(b"nnaes", b"value1".to_vec())?;
        eng.set(b"amhue", b"value2".to_vec())?;
        eng.set(b"meeae", b"value3".to_vec())?;
        eng.set(b"uujeh", b"value4".to_vec())?;
        eng.set(b"anehe", b"value5".to_vec())?;

        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"e".to_vec());

        let mut iter = eng.scan((start.clone(), end.clone()));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"amhue".to_vec());

        let (key2, _) = iter.next().expect("no value founded")?;
        assert_eq!(key2, b"anehe".to_vec());
        drop(iter);

        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());
        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key3, b"uujeh".to_vec());

        let (key4, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key4, b"nnaes".to_vec());

        let (key5, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key5, b"meeae".to_vec());

        path.parent().map(|p| std::fs::remove_dir_all(p));
        Ok(())
    }
}
use std::fs::File;
use std::path::Path;
use anyhow::Result;

const FILE_NAME:  &'static str = "minibitcask.data";
const MERGE_FILE_NAME: &'static str = "minibitcask.data.merge";

struct DBFile {
    file: File,
    offset: u64,
}

impl DBFile {
    fn new<P: AsRef<Path>>(dir_path: P) -> Result<DBFile> {
        let filepath = dir_path.as_ref().join(FILE_NAME); // 转换为 &Path，然后调用 join
        // 在函数内部：
        // 如果 P = &str，需要 as_ref() 转换为 &Path
        // 如果 P = String，需要 as_ref() 转换为 &Path
        // 如果 P = PathBuf，需要 as_ref() 转换为 &Path
        // 如果 P = &Path，as_ref() 返回自身
        DBFile::newInternal(filepath)
    }
    fn newInternal<P: AsRef<Path>>(filepath: P) -> Result<DBFile>  {
        let file = File::open(filepath)?;
        let metadata = file.metadata()?;
        println!("metadata: {:?}", metadata);
        let offset = metadata.len();
        Ok(DBFile { file, offset })
    }
}
fn main() {
    println!("Hello, world!");

}

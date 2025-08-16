use std::path::PathBuf;
use fs4::fs_std::FileExt;
use std::fs::OpenOptions;

fn main() {
    let path = std::env::args().nth(1).expect("missing path argument");
    let path = PathBuf::from(path);

    let file = match OpenOptions::new().read(true).write(true).create(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("lock_probe: failed to open file: {}", e);
            std::process::exit(3);
        }
    };

    match file.try_lock_exclusive() {
        Ok(true) => {
            println!("lock_probe: acquired exclusive lock (unexpected if another process holds it)");
            std::process::exit(0);
        }
        Ok(false) => {
            println!("lock_probe: failed to acquire exclusive lock (expected if another process holds it)");
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("lock_probe: failed to acquire exclusive lock: {}", e);
            std::process::exit(2);
        }
    }
}



use std::sync::{atomic::{AtomicU64, Ordering}};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use once_cell::sync::OnceCell;
use bincode;
use bincode::{Encode, Decode};

pub type KVEngine = BTreeMap<Vec<u8>, Option<Vec<u8>>>;


static VERSION: AtomicU64 = AtomicU64::new(1);

fn add_next_version() -> u64 {
    VERSION.fetch_add(1, Ordering::SeqCst)
}
// 当前活跃的事务 id（version），及其已经写入的 key 信息
pub static ACTIVE_TXN: OnceCell<Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>> = OnceCell::new();

pub struct MVCC {
    kv: Arc<Mutex<KVEngine>>,
}

impl MVCC {
    pub fn new(kv: KVEngine) -> Self {
        Self { kv: Arc::new(Mutex::new(kv)) }
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(self.kv.clone())
    }
}

#[derive(Debug, Encode, Decode)]
struct Key {
    raw_key: Vec<u8>,
    version: u64,
}

impl Key {
    fn encode(&self) -> Vec<u8> {
        bincode::encode_to_vec(self, bincode::config::standard()).unwrap()
    }
}

fn decode_key(b: &Vec<u8>) -> Key {
    let (key, _) = bincode::decode_from_slice(&b, bincode::config::standard()).unwrap();
    key
}

// MVCC 事务
pub struct Transaction {
    // 底层 KV 存储引擎，传入
    kv: Arc<Mutex<KVEngine>>,
    // 事务版本号
    version: u64,
    // 事务启动时的活跃事务列表，version 列表
    active_xid: HashSet<u64>,
}

impl Transaction {
    pub fn begin(kv: Arc<Mutex<KVEngine>>) -> Self {
        let version = add_next_version();

        let mut active_txn = ACTIVE_TXN.get().unwrap().lock().unwrap();
        let active_xid = active_txn.keys().cloned().collect();
        active_txn.insert(version, vec![]);

        
        Self { version, kv, active_xid }
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) {
        self.write(key, Some(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.write(key, None);
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) {
        let mut kvengine = self.kv.lock().unwrap();
        for (enc_key, _) in kvengine.iter().rev() {
            // 同一个key，version大的后面。
            // 逆序遍历，先访问大的version，如果可见，则break
            let key_version = decode_key(enc_key);
            
            if key_version.raw_key.eq(key) {
                if !self.is_visible(key_version.version) {
                    panic!("Transaction is not visible");
                }
                break;
            }
        }

        let mut active_txn = ACTIVE_TXN.get().unwrap().lock().unwrap();
        active_txn.entry(self.version).and_modify(|v| v.push(key.to_vec())).or_insert_with(||vec![key.to_vec()]);

        let enc_key = Key { raw_key: key.to_vec(), version: self.version };
        kvengine.insert(enc_key.encode(), value);
    }
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter().rev() {
            let key_version = decode_key(k);
            if key_version.raw_key.eq(key) && self.is_visible(key_version.version) {
                return v.clone();
            }
        }
        None
    }

    fn print_all(&self) {
        let mut records = BTreeMap::new();
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter() {
            let key_version = decode_key(k);
            if self.is_visible(key_version.version) {
                records.insert(key_version.raw_key.to_vec(), v.clone());
            }
        }
        for (k, v) in records.iter() {
            if let Some(v) = v {
                println!("key: {}, value: {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
            } 
        }
        println!();
    }

    pub fn commit(&self) {
        let mut active_txn = ACTIVE_TXN.get().unwrap().lock().unwrap();
        active_txn.remove(&self.version);
    }

    pub fn rollback(&self) {
        let mut active_txn = ACTIVE_TXN.get().unwrap().lock().unwrap();
        if let Some(keys) = active_txn.get(&self.version) {
            let mut kvengine = self.kv.lock().unwrap();
            for key in keys {
                let res = kvengine.remove(&Key { raw_key: key.to_vec(), version: self.version }.encode());
                assert!(res.is_some());
            }
        }
        active_txn.remove(&self.version);
    }

    // 判断一个版本的数据，对当前事务是否可见
    // 1. 如果是另外一个活跃事务的修改，则不可见
    // 2. 如果版本号比当前大，则不可见
    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }
}

fn main() {
    ACTIVE_TXN.set(Arc::new(Mutex::new(HashMap::new()))).unwrap();
    let eng = KVEngine::new();
    let mvcc = MVCC::new(eng);
    // 先新增几条数据
    let tx0 = mvcc.begin_transaction();
    tx0.set(b"a", b"a1".to_vec());
    tx0.set(b"b", b"b1".to_vec());
    tx0.set(b"c", b"c1".to_vec());
    tx0.set(b"d", b"d1".to_vec());
    tx0.set(b"e", b"e1".to_vec());
    tx0.commit();

    // 开启一个事务
    let tx1 = mvcc.begin_transaction();
    // 将 a 改为 a2，e 改为 e2
    tx1.set(b"a", b"a2".to_vec());
    tx1.set(b"e", b"e2".to_vec());
    // Time
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // t1 虽然未提交，但是能看到自己的修改了
    tx1.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // 开启一个新的事务
    let tx2 = mvcc.begin_transaction();
    // 删除 b
    tx2.delete(b"b");
    // Time
    //  2      X
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // 此时 T1 没提交，所以 T2 看到的是
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1
                     // 提交 T1
    tx1.commit();
    // 此时 T2 仍然看不到 T1 的提交，因为 T2 开启的时候，T2 还没有提交（可重复读）
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1

    // 再开启一个新的事务
    let tx3 = mvcc.begin_transaction();
    // Time
    //  3
    //  2      X               uncommitted
    //  1  a2              e2  committed
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys
    // T3 能看到 T1 的提交，但是看不到 T2 的提交
    tx3.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // T3 写新的数据
    tx3.set(b"f", b"f1".to_vec());
    // T2 写同样的数据，会冲突，然后崩溃
    tx2.set(b"f", b"f1".to_vec());
}

use std::sync::Arc;
use std::path::Path;
use std::io::Write;
use std::convert::TryInto;
use std::cmp::Ordering;

#[cfg(feature="snappy-compression")]
use std::io::Read;

use parking_lot::Mutex;

use lru::LruCache;

use crate::manifest::Manifest;
use crate::Params;
use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;

pub type DataBlockId = u64;

const NUM_SHARDS: usize = 16;

pub struct PrefixedKey {
    prefix_len: u32,
    suffix: Vec<u8>,
}

impl PrefixedKey {
    pub fn new(prefix_len: usize, suffix: Vec<u8>) -> Self {
        Self{ prefix_len: prefix_len as u32, suffix }
    }
}

type BlockShard = LruCache<DataBlockId, Arc<DataBlock>>;

pub struct DataBlocks {
    params: Arc<Params>,
    block_caches: Vec<Mutex<BlockShard>>,
    manifest: Arc<Manifest>,
}

impl DataBlocks {
    pub fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let max_data_files = params.max_open_files / 2;
        let shard_size = max_data_files / NUM_SHARDS;
        assert!(shard_size > 0);

        let mut block_caches = Vec::new();
        for _ in 0..NUM_SHARDS {
            block_caches.push(Mutex::new(BlockShard::new(shard_size)));
        }

        Self{ params, manifest, block_caches }
    }

    #[inline]
    fn block_to_shard_id(block_id: DataBlockId) -> usize {
        (block_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, block_id: &DataBlockId) -> std::path::PathBuf {
        let fname = format!("keys{:08}.lld", block_id);
        self.params.db_path.join(Path::new(&fname))
    }

    pub async fn make_block(&self, entries: Vec<(PrefixedKey, Entry)>, params: &Params) -> DataBlockId {
        let id = self.manifest.next_data_block_id().await;
        let block = Arc::new( DataBlock::new_from_entries(entries, params) );
        let shard_id = Self::block_to_shard_id(id);

        // Store on disk before grabbing the lock
        let block_data = &block.data;
        let fpath = self.get_file_path(&id);

        let mut file = std::fs::File::create(fpath)
            .expect("Failed to store data block on disk");

        #[ cfg(feature="snappy-compression") ]
        {
            use snap::write::FrameEncoder;

            let mut encoder = FrameEncoder::new(file);
            encoder.write_all(block_data)
                .expect("Failed to store data block on disk");
            file = encoder.into_inner().unwrap();
        }

        #[ cfg(not(feature="snappy-compression")) ]
        {
            file.write_all(block_data)
                .expect("Failed to store data block on disk");
        }

        file.sync_all().unwrap();
        log::trace!("Created new data block on disk");

        let mut cache = self.block_caches[shard_id].lock();
        cache.put(id, block);

        id
    }

    pub fn get_block(&self, id: &DataBlockId) -> Arc<DataBlock> {
        let shard_id = Self::block_to_shard_id(*id);

        let mut cache = self.block_caches[shard_id].lock();
        if let Some(block) = cache.get(id) {
            block.clone()
        } else {
            log::trace!("Loading data block from disk");
            let fpath = self.get_file_path(&id);
            let data;

            #[ cfg(feature="snappy-compression") ]
            {
                use snap::read::FrameDecoder;

                let file = std::fs::File::open(fpath)
                    .expect("Failed to open data block file");
                let mut decoder = FrameDecoder::new(file);

                let mut rdata = Vec::new();
                decoder.read_to_end(&mut rdata).unwrap();

                data = rdata;
            }

            #[ cfg(not(feature="snappy-compression")) ]
            {
                data = std::fs::read(fpath)
                    .expect("Cannot read data block from disk");
            }

            let block = Arc::new(DataBlock::new_from_data(data));

            cache.put(*id, block.clone());
            block
        }
    }
}


/*
 * The data layout is the following:
 * 1. 4 bytes marking where the restart list starts
 * 2. Variable length of Block Entries, where each entry is:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Variable length key suffix
 *  - Fixed size value reference
 * 3. Variable length or restart list (each entry is 4bytes; so we dont need length information)
 */
pub struct DataBlock {
    restart_list_start: usize,
    data: Vec<u8>
}

impl DataBlock {
    pub fn new_from_entries(mut entries: Vec<(PrefixedKey, Entry)>, params: &Params) -> Self {
        let mut data = vec![];

        // The restart list keeps track of when the keys are fully reset
        // This enables using binary search in get() instead of seeking linearly
        let mut restart_list = vec![];

        // Reserve space to mark where the restart list starts
        data.append(&mut 0u32.to_le_bytes().to_vec());

        for (pos, (mut key, entry)) in entries.drain(..).enumerate() {
            if pos % params.block_restart_interval == 0 {
                assert!(key.prefix_len == 0);
                restart_list.push(data.len() as u32);
            }

            let mut pkey_len = (key.prefix_len   as u32).to_le_bytes().to_vec();
            let mut skey_len = (key.suffix.len() as u32).to_le_bytes().to_vec();

            data.append(&mut pkey_len);
            data.append(&mut skey_len);
            data.append(&mut key.suffix);

            let mut entry_data = bincode::serialize(&entry).unwrap();
            data.append(&mut entry_data);
        }

        let rl_len_len = std::mem::size_of::<u32>();
        let restart_list_start = data.len() as u32;
        data[..rl_len_len].copy_from_slice(&restart_list_start.to_le_bytes());

        for restart_offset in restart_list.drain(..) {
            let mut offset = restart_offset.to_le_bytes().to_vec();
            data.append(&mut offset);
        }

        Self{ data, restart_list_start: restart_list_start as usize }
    }

    pub fn new_from_data(data: Vec<u8>) -> Self {
        let rls_len = std::mem::size_of::<u32>();
        let restart_list_start = u32::from_le_bytes(data[..rls_len].try_into().unwrap());
        Self{ data, restart_list_start: restart_list_start as usize }
    }

    /// Get the key and entry at the specified offset (must be valid!)
    /// The third entry in this result is the new offset after the entry
    #[inline]
    pub fn get_offset(&self, offset: u32, previous_key: &[u8]) -> (Key, Entry, u32) {
        let rl_len_len = std::mem::size_of::<u32>();
        let mut offset = (offset as usize) + rl_len_len;

        assert!(offset < self.restart_list_start);

        let len_len = std::mem::size_of::<u32>();
        let pkey_len = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        let skey_len = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        let kdata = [&previous_key[..pkey_len as usize], &self.data[offset..offset+(skey_len as usize)]].concat();
        offset += skey_len as usize;

        let entry_len = bincode::serialized_size(&Entry::default()).unwrap() as usize;
        let entry = bincode::deserialize(&self.data[offset..offset+entry_len]).unwrap();

        offset += entry_len;
        offset -= rl_len_len;

        (kdata, entry, offset as u32)
    }

    // Length of this block in bytes (without the reset list)
    pub fn byte_len(&self) -> u32 {
        // "Cut-off" the beginning and end
        let rl_len_len = std::mem::size_of::<u32>();
        let rl_len = self.data.len() - self.restart_list_start;

        (self.data.len() - rl_len_len - rl_len) as u32
    }

    #[inline]
    fn restart_list_len(&self) -> usize {
        let offset_len = std::mem::size_of::<u32>();
        let rl_len = self.data.len() - self.restart_list_start;

        assert!(rl_len % offset_len == 0);
        rl_len / offset_len
    }

    #[inline]
    fn get_restart_offset(&self, pos: u32) -> u32 {
        let rl_len_len = std::mem::size_of::<u32>() as u32;
        let offset_len = std::mem::size_of::<u32>();

        let pos = self.restart_list_start + (pos as usize)*offset_len;

        u32::from_le_bytes(self.data[pos..pos+offset_len].try_into().unwrap()) - rl_len_len
    }

    pub fn get(&self, key: &[u8]) -> Option<(u64, ValueId)> {
        let rl_len = self.restart_list_len();

        let mut start: u32 = 0;
        let mut end = (rl_len as u32) - 1;

        // binary search
        while end-start > 1 {
            let mid = start + (end - start) / 2;
            let offset = self.get_restart_offset(mid);
            let (this_key, entry,  _) = self.get_offset(offset, &[]);

            match this_key.as_slice().cmp(key) {
                Ordering::Equal => {
                    // exact match
                    return Some((entry.seq_number, entry.value_ref));
                }
                Ordering::Less => {
                    // continue with right half
                    start = mid;
                }
                Ordering::Greater => {
                    // continue with left half
                    end = mid;
                }
            }
        }

        // do a sequential search for the remaining interval
        let mut pos = self.get_restart_offset(start) as u32;

        // There is no reset at the very end so we need to include
        // that part in the sequential search
        let end_offset = if end+1 == rl_len as u32 {
            self.byte_len() 
        } else {
            self.get_restart_offset(end) as u32
        };

        let mut last_key = vec![];
        while pos < end_offset {
            let (this_key, entry, new_pos) = self.get_offset(pos, &last_key);

            if key == this_key {
                return Some((entry.seq_number, entry.value_ref));
            }

            pos = new_pos as u32;
            last_key = this_key;
        }

        // Not found
        None
    }
}

#[ cfg(test) ]
mod tests {
    use super::*;

    #[test]
    fn store_and_load() {
        let key1 = PrefixedKey{ prefix_len: 0, suffix: vec![5] };
        let entry1 = Entry{ seq_number: 14234524, value_ref: (4,2) };

        let key2 = PrefixedKey{ prefix_len: 1, suffix: vec![2] };
        let entry2 = Entry{ seq_number: 424234, value_ref: (4,50) };

        let entries = vec![(key1, entry1.clone()), (key2, entry2.clone())];

        let params = Params::default();

        let data_block = DataBlock::new_from_entries(entries, &params);

        let data_block2 = DataBlock::new_from_data(data_block.data.clone());

        let prev_key = vec![];
        let (key, val, pos) = data_block2.get_offset(0, &prev_key);

        assert_eq!(key, vec![5]);
        assert_eq!(val, entry1);

        let (key, val, pos) = data_block2.get_offset(pos, &key);

        assert_eq!(key, vec![5, 2]);
        assert_eq!(val, entry2);
        assert_eq!(pos, data_block2.byte_len());
    }
}

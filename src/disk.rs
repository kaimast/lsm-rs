#[cfg(feature="async-io")]
use tokio::fs;

#[cfg(not(feature="async-io"))]
use std::fs;

#[cfg(feature="async-io")]
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[cfg(not(feature="async-io"))]
use std::io::{Read, Seek, Write};

use std::path::Path;

use cfg_if::cfg_if;

//TODO add proper error handling

pub async fn read(fpath: &Path, offset: u64) -> Vec<u8> {
    let mut compressed = vec![];

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let mut file = fs::File::open(fpath).await
                    .expect("Failed to open file for reading");

            if offset > 0 {
                file.seek(futures::io::SeekFrom::Start(offset)).await.unwrap();
            }

            match file.read_to_end(&mut compressed).await {
                Ok(_) => {}
                Err(e) => {
                    panic!("Cannot read file {:?} from disk: {}", fpath, e);
                }
            };
        } else {
            let mut file = fs::File::open(fpath)
                    .expect("Failed to open file for reading");

            if offset > 0 {
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
            }

            match file.read_to_end(&mut compressed) {
                Ok(_) => {}
                Err(e) => {
                    panic!("Cannot read file {:?} from disk: {}", fpath, e);
                }
            };
        }
    }

    cfg_if! {
        if #[ cfg(feature="snappy-compression") ] {
            let mut decoder = snap::raw::Decoder::new();
            decoder.decompress_vec(&compressed)
                .expect("Failed to decompress data")
        } else {
            compressed
        }
    }
}

pub async fn write(fpath: &Path, data: &[u8], offset: u64) {
    //TODO it might be worth investigating if encoding/decoding
    // chunks is more efficient

    let compressed;

    cfg_if! {
        if #[cfg(feature="snappy-compression") ] {
            let mut encoder = snap::raw::Encoder::new();
            compressed = encoder.compress_vec(data)
                .expect("Failed to compress data");
        } else {
            compressed = data;
        }
    }

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let mut file = fs::OpenOptions::new().create(true).write(true)
                .open(fpath).await.expect("Failed to open file for writing");

            if offset > 0 {
                file.set_len(offset).await;
                file.seek(futures::io::SeekFrom::Start(offset)).await;
            }

            file.write_all(&compressed).await
                    .expect("Failed to write to file");

            file.sync_all().await.expect("Failed to write to file");
        } else {
            let mut file = fs::OpenOptions::new().create(true).write(true)
                .open(fpath).expect("Failed to open file for writing");

            if offset > 0 {
                file.set_len(offset).unwrap();
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
            }

            file.write_all(&compressed)
                    .expect("Failed to write to file");

            file.sync_all().expect("Failed to write to file");
        }
    }
}

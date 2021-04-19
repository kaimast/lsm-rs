#[cfg(feature="async-io")]
use tokio::fs;

#[cfg(not(feature="async-io"))]
use std::fs;

#[cfg(feature="async-io")]
use tokio::io::AsyncWriteExt;

#[cfg(not(feature="async-io"))]
use std::io::Write;

use std::path::Path;

use cfg_if::cfg_if;

#[ cfg(feature="snappy-compression") ]
pub async fn read(fpath: &Path) -> Vec<u8> {
    let mut decoder = snap::raw::Decoder::new();

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let compressed = match fs::read(fpath).await {
                Ok(data) => data,
                Err(e) => {
                    panic!("Cannot read file {:?} from disk: {}", fpath, e);
                }
            };
        } else {
            let compressed = match fs::read(fpath) {
                Ok(data) => data,
                Err(e) => {
                    panic!("Cannot read file {:?} from disk: {}", fpath, e);
                }
            };
        }
    }

    decoder.decompress_vec(&compressed)
        .expect("Failed to decompress data")
}

#[ cfg(not(feature="snappy-compression")) ]
pub async fn read(fpath: &Path) -> Vec<u8> {
    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            fs::read(fpath).await
                    .expect("Cannot read file from disk")
        } else {
            fs::read(fpath)
                    .expect("Cannot read file from disk")
        }
    }
}

#[ cfg(feature="snappy-compression") ]
pub async fn write(fpath: &Path, data: &[u8]) {
    //TODO it might be worth investigating if encoding/decoding
    // chunks is more efficient

    let mut encoder = snap::raw::Encoder::new();
    let compressed = encoder.compress_vec(data)
        .expect("Failed to compress data");

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let mut file = fs::File::create(fpath).await
                    .expect("Failed to open file for writing");

            file.write_all(&compressed).await
                    .expect("Failed to write to file");

            file.sync_all().await.expect("Failed to write to file");
        } else {
            let mut file = fs::File::create(fpath)
                    .expect("Failed to open file for writing");

            file.write_all(&compressed)
                    .expect("Failed to write to file");

            file.sync_all().expect("Failed to write to file");
        }
    }
}

#[ cfg(not(feature="snappy-compression")) ]
pub async fn write(fpath: &Path, data: &[u8]) {
    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let mut file = fs::File::create(fpath).await
                    .expect("Failed to open file for writing");

            file.write_all(data).await
                    .expect("Failed to write to file");

            file.sync_all().await.expect("Failed to write to file");
        } else {
            let mut file = fs::File::create(fpath)
                    .expect("Failed to open file for writing");

            file.write_all(data)
                    .expect("Failed to write to file");

            file.sync_all().expect("Failed to write to file");
        }
    }
}

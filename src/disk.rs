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

#[ inline(always) ]
pub async fn read(fpath: &Path, offset: u64) -> Result<Vec<u8>, std::io::Error> {
    let mut compressed = vec![];

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let mut file = fs::File::open(fpath).await?;

            if offset > 0 {
                file.seek(futures::io::SeekFrom::Start(offset)).await?;
            }

            file.read_to_end(&mut compressed).await?;
        } else {
            let mut file = fs::File::open(fpath)?;

            if offset > 0 {
                file.seek(std::io::SeekFrom::Start(offset))?;
            }

            file.read_to_end(&mut compressed)?;
        }
    }

    cfg_if! {
        if #[ cfg(feature="snappy-compression") ] {
            let mut decoder = snap::raw::Decoder::new();
            Ok(decoder.decompress_vec(&compressed)?)
        } else {
            Ok(compressed)
        }
    }
}

#[ inline(always) ]
pub async fn write(fpath: &Path, data: &[u8], offset: u64) -> Result<(), std::io::Error> {
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
                .open(fpath).await?;

            if offset > 0 {
                file.set_len(offset).await?;
                file.seek(futures::io::SeekFrom::Start(offset)).await?;
            }

            file.write_all(&compressed).await?;
            file.sync_all().await?;
        } else {
            let mut file = fs::OpenOptions::new().create(true).write(true)
                .open(fpath)?;

            if offset > 0 {
                file.set_len(offset)?;
                file.seek(std::io::SeekFrom::Start(offset))?;
            }

            file.write_all(&compressed)?;
            file.sync_all()?;
        }
    }

    Ok(())
}

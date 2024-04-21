#[cfg(feature = "async-io")]
use tokio_uring::fs;

#[cfg(not(feature = "async-io"))]
use std::fs;

#[cfg(not(feature = "async-io"))]
use std::io::{Read, Seek, Write};

use std::path::Path;

use cfg_if::cfg_if;

#[inline(always)]
#[tracing::instrument]
pub async fn read(fpath: &Path, offset: u64) -> Result<Vec<u8>, std::io::Error> {
    let mut compressed = vec![];

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let file = fs::File::open(fpath).await?;
            let (res, buf) = file.read_at_to_end(offset, compressed).await;
            res?;
            compressed = buf;
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

#[tracing::instrument(skip(data))]
#[inline(always)]
pub async fn write(fpath: &Path, data: &[u8], offset: u64) -> Result<(), std::io::Error> {
    //TODO it might be worth investigating if encoding/decoding
    // chunks is more efficient

    cfg_if! {
        if #[cfg(feature="snappy-compression") ] {
            let mut encoder = snap::raw::Encoder::new();
            let compressed = encoder.compress_vec(data)
                .expect("Failed to compress data");
        } else {
            let mut compressed = vec![];
            compressed.extend_from_slice(data);
        }
    }

    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let file = fs::OpenOptions::new().create(true).write(true)
                .open(fpath).await?;

            let (res, _buf) = file.write_all_at(compressed, offset).await;
            res?;
            file.sync_all().await?;
        } else {
            let mut file = fs::OpenOptions::new().create(true)
                .truncate(true).write(true)
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

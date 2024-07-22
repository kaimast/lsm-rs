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
pub async fn read_uncompressed(fpath: &Path, offset: u64) -> Result<Vec<u8>, std::io::Error> {
    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let file = fs::File::open(fpath).await?;
            let (res, buf) = file.read_at_to_end(offset, vec![]).await;
            res?;
        } else {
            let mut file = fs::File::open(fpath)?;

            if offset > 0 {
                file.seek(std::io::SeekFrom::Start(offset))?;
            }

            let mut buf = vec![];
            file.read_to_end(&mut buf)?;

        }
    }

    Ok(buf)
}

#[inline(always)]
#[tracing::instrument]
pub async fn read(fpath: &Path, offset: u64) -> Result<Vec<u8>, std::io::Error> {
    let compressed = read_uncompressed(fpath, offset).await?;

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
pub async fn write(fpath: &Path, data: &[u8]) -> Result<(), std::io::Error> {
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

    write_uncompressed(fpath, compressed).await
}

#[tracing::instrument(skip(data))]
#[inline(always)]
pub async fn write_uncompressed(fpath: &Path, data: Vec<u8>) -> Result<(), std::io::Error> {
    cfg_if! {
        if #[ cfg(feature="async-io") ] {
            let file = fs::OpenOptions::new().create(true)
                .truncate(true).write(true)
                .open(fpath).await?;

            let (res, _buf) = file.write_all_at(data, 0).await;
            res?;
            file.sync_all().await?;
        } else {
            let mut file = fs::OpenOptions::new().create(true)
                .truncate(true).write(true)
                .open(fpath)?;

            file.write_all(&data)?;
            file.sync_all()?;
        }
    }

    Ok(())
}

use tokio::fs;
use tokio::io::AsyncWriteExt;

use std::path::Path;

#[ cfg(feature="snappy-compression") ]
pub async fn read(fpath: &Path) -> Vec<u8> {
    let mut decoder = snap::raw::Decoder::new();
    let compressed =fs::read(fpath).await
            .expect("Cannot read file from disk");

    decoder.decompress_vec(&compressed)
        .expect("Failed to decompress data")
}

#[ cfg(not(feature="snappy-compression")) ]
pub async fn read(fpath: &Path) -> Vec<u8> {
    fs::read(fpath).await
            .expect("Cannot read file from disk")
}

#[ cfg(feature="snappy-compression") ]
pub async fn write(fpath: &Path, data: &[u8]) {
    //TODO it might be worth investigating if encoding/decoding
    // chunks is more efficient

    let mut encoder = snap::raw::Encoder::new();
    let compressed = encoder.compress_vec(data)
        .expect("Failed to compress data");

    let mut file = fs::File::create(fpath).await
            .expect("Failed to open file for writing");

    file.write_all(&compressed).await
            .expect("Failed to write to file");

    file.sync_all().await.expect("Failed to write to file");
}

#[ cfg(not(feature="snappy-compression")) ]
pub async fn write(fpath: &Path, data: &[u8]) {
    let mut file = fs::File::create(fpath).await
            .expect("Failed to open file for writing");

    file.write_all(data).await
            .expect("Failed to write to file");

    file.sync_all().await.expect("Failed to write to file");
}

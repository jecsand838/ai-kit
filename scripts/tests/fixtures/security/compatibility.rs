use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::CompressionType;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use include_flate_compress::{CompressionMethod, apply_compression, apply_decompression};
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, ObjectStore, RetryConfig};

struct Compatibility;

impl Compatibility {
    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(
            std::env::var_os("AI_KIT_SECURITY_FIXTURES").expect("legacy fixture directory"),
        )
        .join(name)
    }

    fn payload() -> Vec<u8> {
        fs::read(Self::fixture("payload.txt")).unwrap()
    }

    fn batch() -> RecordBatch {
        let ids = Int64Array::from(
            (0..1024)
                .map(|i| (i % 7 != 0).then_some(i))
                .collect::<Vec<_>>(),
        );
        let text = StringArray::from(
            (0..1024)
                .map(|i| {
                    (i % 11 != 0)
                        .then(|| format!("row-{}-{}", i % 3, "portable storage α & β ".repeat(16)))
                })
                .collect::<Vec<_>>(),
        );
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids), Arc::new(text)]).unwrap()
    }
}

#[test]
fn zstd_reads_pre_update_frame_and_round_trips() {
    let payload = Compatibility::payload();
    let legacy = fs::read(Compatibility::fixture("zstd-0.13.3.zst")).unwrap();
    assert_eq!(
        zstd::stream::decode_all(legacy.as_slice()).unwrap(),
        payload
    );
    let current = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
    assert_eq!(
        zstd::stream::decode_all(current.as_slice()).unwrap(),
        payload
    );
    assert!(zstd::stream::decode_all(&legacy[..legacy.len() - 1]).is_err());
}

#[test]
fn include_flate_reads_pre_update_frame_and_round_trips() {
    let payload = Compatibility::payload();
    let legacy = fs::read(Compatibility::fixture("include-flate-compress-0.3.2.zst")).unwrap();
    let mut decoded = Vec::new();
    apply_decompression(legacy.as_slice(), &mut decoded, CompressionMethod::Zstd).unwrap();
    assert_eq!(decoded, payload);
    let mut compressed = Vec::new();
    apply_compression(
        &mut payload.as_slice(),
        &mut compressed,
        CompressionMethod::Zstd,
    )
    .unwrap();
    let mut decoded = Vec::new();
    apply_decompression(compressed.as_slice(), &mut decoded, CompressionMethod::Zstd).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn arrow_reads_pre_update_compressed_ipc_and_round_trips() {
    let expected = Compatibility::batch();
    let legacy = File::open(Compatibility::fixture("arrow-ipc-58.3.0-zstd.arrow")).unwrap();
    let mut reader = FileReader::try_new(legacy, None).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), expected);
    assert!(reader.next().is_none());
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        FileWriter::try_new_with_options(&mut bytes, expected.schema().as_ref(), options).unwrap();
    writer.write(&expected).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), expected);
    assert!(reader.next().is_none());
}

struct XmlResponse {
    address: SocketAddr,
    worker: Option<JoinHandle<std::io::Result<()>>>,
}

impl XmlResponse {
    fn serve(xml: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => return Err(error),
                }
            };
            socket.set_read_timeout(Some(Duration::from_secs(3)))?;
            socket.set_write_timeout(Some(Duration::from_secs(3)))?;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") && request.len() < 8192 {
                let mut byte = [0];
                socket.read_exact(&mut byte)?;
                request.push(byte[0]);
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with("GET /fixture-bucket?"), "{request}");
            assert!(request.contains("list-type=2"), "{request}");
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
                xml.len()
            )?;
            socket.flush()
        });
        Self {
            address,
            worker: Some(worker),
        }
    }

    fn store(&self) -> object_store::aws::AmazonS3 {
        AmazonS3Builder::new()
            .with_bucket_name("fixture-bucket")
            .with_region("us-east-1")
            .with_endpoint(format!("http://{}", self.address))
            .with_allow_http(true)
            .with_skip_signature(true)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(3))
                    .with_proxy_excludes("127.0.0.1"),
            )
            .build()
            .unwrap()
    }

    fn finish(mut self) {
        self.worker.take().unwrap().join().unwrap().unwrap();
    }
}

impl Drop for XmlResponse {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[tokio::test]
async fn object_store_reads_escaped_s3_xml() {
    let server = XmlResponse::serve(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Contents><Key>records/a&amp;b.xml</Key><Size>37</Size><LastModified>2026-01-01T00:00:00Z</LastModified><ETag>&quot;etag-1&quot;</ETag></Contents>
<CommonPrefixes><Prefix>nested&amp;safe/</Prefix></CommonPrefixes>
</ListBucketResult>"#,
    );
    let result = tokio::time::timeout(
        Duration::from_secs(4),
        server.store().list_with_delimiter(None),
    )
    .await
    .unwrap()
    .unwrap();
    server.finish();
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].location.as_ref(), "records/a&b.xml");
    assert_eq!(result.objects[0].size, 37);
    assert_eq!(result.objects[0].e_tag.as_deref(), Some("\"etag-1\""));
    assert_eq!(result.common_prefixes[0].as_ref(), "nested&safe");
}

#[tokio::test]
async fn object_store_rejects_malformed_xml_and_unresolved_entities() {
    for xml in [
        "<ListBucketResult><Contents><Key>broken</Contents></ListBucketResult>",
        "<ListBucketResult><Contents><Key>&undefined;</Key><Size>1</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>",
    ] {
        let server = XmlResponse::serve(xml);
        let result = tokio::time::timeout(
            Duration::from_secs(4),
            server.store().list_with_delimiter(None),
        )
        .await
        .unwrap();
        server.finish();
        assert!(result.is_err(), "malformed XML must remain an error: {xml}");
    }
}

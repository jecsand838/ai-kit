use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use include_flate_compress::{CompressionMethod, apply_compression};

struct LegacyFixtures;

impl LegacyFixtures {
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

    fn write(directory: &Path) {
        fs::create_dir_all(directory).unwrap();
        let payload = "pre-update zstd compatibility: αβ & <xml>\n"
            .repeat(2048)
            .into_bytes();
        fs::write(directory.join("payload.txt"), &payload).unwrap();
        let compressed = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        fs::write(directory.join("zstd-0.13.3.zst"), compressed).unwrap();
        let mut compressed = Vec::new();
        apply_compression(
            &mut payload.as_slice(),
            &mut compressed,
            CompressionMethod::Zstd,
        )
        .unwrap();
        fs::write(
            directory.join("include-flate-compress-0.3.2.zst"),
            compressed,
        )
        .unwrap();
        let batch = Self::batch();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap();
        let file = File::create(directory.join("arrow-ipc-58.3.0-zstd.arrow")).unwrap();
        let mut writer =
            FileWriter::try_new_with_options(file, batch.schema().as_ref(), options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
}

fn main() {
    let directory = std::env::args_os().nth(1).expect("fixture destination");
    LegacyFixtures::write(Path::new(&directory));
}

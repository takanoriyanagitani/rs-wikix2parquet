use std::fs::File;
use std::io;
use std::process::ExitCode;
use std::sync::Arc;

use io::Write;

use rs_wikix2parquet::arrow;
use rs_wikix2parquet::parquet;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use parquet::basic::Compression;

use rs_wikix2parquet::ix::IndexEntry;
use rs_wikix2parquet::rows2batches::IndexBatchIter;

fn schema() -> Schema {
    IndexEntry::schema()
}

fn stdin2entries() -> impl Iterator<Item = Result<IndexEntry, io::Error>> {
    rs_wikix2parquet::ix::stdin2entries()
}

fn items2iter<I>(
    items: I,
    schema: Arc<Schema>,
    bsize: usize,
) -> impl Iterator<Item = Result<RecordBatch, io::Error>>
where
    I: Iterator<Item = Result<IndexEntry, io::Error>>,
{
    IndexBatchIter {
        inner: items,
        schema,
        batch_size: bsize,
    }
}

fn iter2file<I>(i: I, mut f: File, c: Compression, s: Arc<Schema>) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    rs_wikix2parquet::wtr::iter2wtr(i, &mut f, c, s)?;
    f.flush()?;
    f.sync_data()
}

fn str2comp(s: &str) -> Result<Compression, io::Error> {
    match s {
        "raw" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "lzo" => Ok(Compression::LZO),
        "lz4" => Ok(Compression::LZ4_RAW),
        _ => Err(io::Error::other(format!("unsupported compression: {s}"))),
    }
}

fn env2filename() -> Result<String, io::Error> {
    std::env::var("OUT_FILENAME").map_err(|_| io::Error::other("missing OUT_FILENAME"))
}

fn env2compression() -> Result<Compression, io::Error> {
    let val = std::env::var("COMPRESSION").unwrap_or_else(|_| "raw".to_string());
    str2comp(&val)
}

fn env2bsize() -> Result<usize, io::Error> {
    let val = std::env::var("BATCH_SIZE").unwrap_or_else(|_| "1024".to_string());
    str::parse(val.as_str()).map_err(|e| io::Error::other(format!("invalid batch size: {e}")))
}

fn sub() -> Result<(), io::Error> {
    let filename: String = env2filename()?;
    let comp: Compression = env2compression()?;
    let bsize: usize = env2bsize()?;

    let sch: Arc<Schema> = Arc::new(schema());

    let rows = stdin2entries();
    let batches = items2iter(rows, sch.clone(), bsize);

    let f = File::create(&filename)?;
    iter2file(batches, f, comp, sch)?;

    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}

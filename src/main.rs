use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::Parser;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::prelude::*;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    urls_file: PathBuf,
    working_directory: PathBuf,
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let download_dir = args.working_directory.join("downloads");
    let output_dir = args.working_directory.join("output");
    tokio::fs::create_dir_all(&args.working_directory).await?;
    tokio::fs::create_dir_all(&download_dir).await?;
    tokio::fs::create_dir_all(&output_dir).await?;

    let urls_file = BufReader::new(File::open(&args.urls_file).await?);
    let mut lines = urls_file.lines();
    let mut urls = vec![];

    while let Some(line) = lines.next_line().await? {
        urls.push(line);
    }

    for (idx, url) in urls.iter().enumerate() {
        let path = download_dir.join(format!("url-{}.parquet", idx));
        let output_dir = output_dir.join(format!("url-{}/", idx));
        download_file(&url, &path).await?;
        get_unique_python_files(&path, &output_dir, include_str!("../sql/unique_files.sql")).await?;
        tokio::fs::remove_file(&path).await?;
    }
    get_unique_python_files(Path::new("data/combined.parquet"), Path::new("data/combined/"), include_str!("../sql/unique_files_combined.sql")).await?;
    Ok(())
}


async fn download_file(url: &str, path: &Path) -> Result<()> {
    let file = File::create(path).await?;
    println!("Downloading {} to {}", url, path.display());

    let response = reqwest::get(url)
        .await?.error_for_status()?;

    let content_length = response.content_length().ok_or_else(|| anyhow!("No content-length set"))?;
    let pbar = ProgressBar::new(content_length);
    pbar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-"));
    pbar.enable_steady_tick(Duration::from_secs(1));

    let mut writer = BufWriter::new(pbar.wrap_async_write(file));

    let mut stream = response.bytes_stream();

    while let Some(item) = stream.next().await {
        writer.write_all(&item?).await?;
    }

    writer.flush().await?;
    pbar.finish();

    println!("Downloaded {} to {}", url, path.display());
    Ok(())
}

async fn get_unique_python_files(path: &Path, output: &Path, sql: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let read_options = ParquetReadOptions::default().parquet_pruning(true);
    ctx.register_parquet("input_dataset", path.to_str().unwrap(), read_options).await?;

    let df = ctx.sql(sql).await.unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(13).unwrap()))
        .set_sorting_columns(Some(vec![SortingColumn::new(0, true, true)]))
        .set_column_encoding(ColumnPath::new(vec!["hash".into()]), Encoding::DELTA_BYTE_ARRAY)
        .build();
    df.write_parquet(
        output.to_str().unwrap(),
        Some(props),
    ).await?;
    Ok(())
}

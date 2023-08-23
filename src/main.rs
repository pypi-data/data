use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::Parser;
use datafusion::parquet;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::column::writer::ColumnCloseResult;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::writer::SerializedFileWriter;
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

    #[arg(short, long)]
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let download_dir = args.working_directory.join("downloads");
    let output_dir = args.working_directory.join("output");
    let combined_parquet_file = args.working_directory.join("combined.parquet");
    let final_output_dir = args.working_directory.join("final");
    tokio::fs::create_dir_all(&args.working_directory).await?;
    tokio::fs::create_dir_all(&download_dir).await?;
    tokio::fs::create_dir_all(&output_dir).await?;
    // tokio::fs::create_dir_all(&final_output_dir).await?;

    let urls_file = BufReader::new(File::open(&args.urls_file).await?);
    let mut lines = urls_file.lines();
    let mut urls = vec![];

    while let Some(line) = lines.next_line().await? {
        urls.push(line);
    }

    let urls = match args.limit {
        None => urls,
        Some(l) => urls.into_iter().take(l).collect(),
    };

    for (idx, url) in urls.into_iter().enumerate() {
        let path = download_dir.join(format!("url-{}.parquet", idx));
        let output_dir = output_dir.join(format!("url-{}/", idx));
        download_file(&url, &path).await?;
        get_unique_python_files(&path, &output_dir, include_str!("../sql/unique_files.sql"))
            .await?;
        tokio::fs::remove_file(&path).await?;
    }

    let all_files: Vec<_> = glob::glob(&format!("{}/**/*.parquet", output_dir.display()))
        .unwrap()
        .flatten()
        .collect();
    let combined_parquet_file_cloned = combined_parquet_file.clone();
    tokio::task::spawn_blocking(move || {
        combine_parquet_files(&all_files, &combined_parquet_file_cloned)
    })
    .await??;

    get_unique_python_files(
        &combined_parquet_file,
        &final_output_dir,
        include_str!("../sql/unique_files_combined.sql"),
    )
    .await?;

    Ok(())
}

async fn download_file(url: &str, path: &Path) -> Result<()> {
    let file = File::create(path).await?;
    println!("Downloading {} to {}", url, path.display());

    let response = reqwest::get(url).await?.error_for_status()?;

    let content_length = response
        .content_length()
        .ok_or_else(|| anyhow!("No content-length set"))?;
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
    ctx.register_parquet("input_dataset", path.to_str().unwrap(), read_options)
        .await?;

    let df = ctx.sql(sql).await.unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(13).unwrap()))
        .set_sorting_columns(Some(vec![SortingColumn::new(0, true, true)]))
        .set_column_encoding(
            ColumnPath::new(vec!["hash".into()]),
            Encoding::DELTA_BYTE_ARRAY,
        )
        .build();
    df.write_parquet(output.to_str().unwrap(), Some(props))
        .await?;
    Ok(())
}

fn combine_parquet_files(files: &[PathBuf], output: &Path) -> Result<()> {
    let output = std::fs::File::create(output)?;

    let inputs = files
        .iter()
        .map(|x| {
            let reader = std::fs::File::open(x)?;
            let metadata = parquet::file::footer::parse_metadata(&reader)?;
            Ok((reader, metadata))
        })
        .collect::<Result<Vec<_>>>()?;

    let expected = inputs[0].1.file_metadata().schema();
    for (_, metadata) in inputs.iter().skip(1) {
        let actual = metadata.file_metadata().schema();
        if expected != actual {
            return Err(anyhow!(
                "inputs must have the same schema, {expected:#?} vs {actual:#?}"
            ));
        }
    }

    let props = Arc::new(WriterProperties::builder().build());
    let schema = inputs[0].1.file_metadata().schema_descr().root_schema_ptr();
    let mut writer = SerializedFileWriter::new(output, schema, props)?;

    for (input, metadata) in inputs {
        for rg in metadata.row_groups() {
            let mut rg_out = writer.next_row_group()?;
            for column in rg.columns() {
                let result = ColumnCloseResult {
                    bytes_written: column.compressed_size() as _,
                    rows_written: rg.num_rows() as _,
                    metadata: column.clone(),
                    bloom_filter: None,
                    column_index: None,
                    offset_index: None,
                };
                rg_out.append_column(&input, result)?;
            }
            rg_out.close()?;
        }
    }

    writer.close()?;
    Ok(())
}

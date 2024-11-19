use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;
use serde_json::json;
use reqwest::{Client, header};
use tokio;
use clap::Parser;
use base64;

/// CLI for creating an OpenSearch index from a CSV file
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Name of the index to be created in OpenSearch
    #[clap(short, long)]
    index_name: String,

    /// Path to the CSV file
    #[clap(short, long)]
    file_path: String,

    /// Username for OpenSearch authentication
    #[clap(short, long)]
    username: String,

    /// Password for OpenSearch authentication
    #[clap(short, long)]
    password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let file = File::open(&args.file_path)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    let headers = rdr.headers()?.clone();

    let client = Client::new();

    // Create the index
    let index_url = format!("http://localhost:9200/{}", args.index_name);
    let auth_header = format!("Basic {}", base64::encode(format!("{}:{}", args.username, args.password)));
    let response = client.put(&index_url)
        .header(header::AUTHORIZATION, auth_header.clone())
        .send()
        .await?;
    if response.status().is_success() {
        println!("Index created successfully.");
    } else {
        println!("Failed to create index: {}", response.text().await?);
        return Err("Failed to create index".into());
    }

    for result in rdr.records() {
        let record = result?;
        let mut json_record = serde_json::Map::new();

        for (header, field) in headers.iter().zip(record.iter()) {
            json_record.insert(header.to_string(), json!(field));
        }

        // Index the document
        let response = client.post(&format!("{}/_doc", index_url))
            .header(header::AUTHORIZATION, auth_header.clone())
            .json(&json_record)
            .send()
            .await?;
        if !response.status().is_success() {
            println!("Failed to index document: {}", response.text().await?);
        }
    }

    Ok(())
}


use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(default)]
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub rows_affected: u64,
    #[serde(default)]
    pub execution_time_ms: u64,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

pub async fn execute_query(host: &str, port: u16, sql: &str) -> Result<()> {
    let url = format!("http://{}:{}/_query", host, port);

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&QueryRequest {
            sql: sql.to_string(),
            params: vec![],
        })
        .send()
        .await?;

    let result: QueryResponse = response.json().await?;

    if result.success {
        if !result.data.is_empty() {
            println!("{}", serde_json::to_string_pretty(&result.data)?);
        } else {
            println!("Success. Rows affected: {}", result.rows_affected);
        }
    } else if let Some(error) = result.error {
        eprintln!("Error: {}", error);
    }

    Ok(())
}

pub async fn dump(host: &str, port: u16) -> Result<String> {
    let url = format!("http://{}:{}/_dump", host, port);
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?;
    let sql = response.text().await?;
    Ok(sql)
}

pub async fn restore(host: &str, port: u16, sql: &str) -> Result<()> {
    let url = format!("http://{}:{}/_restore", host, port);
    let client = reqwest::Client::new();
    let response = client.post(&url).body(sql.to_string()).send().await?;
    let result: QueryResponse = response.json().await?;

    if result.success {
        println!(
            "Restore completed successfully. Rows affected: {}",
            result.rows_affected
        );
    } else if let Some(error) = result.error {
        eprintln!("Restore error: {}", error);
    }
    Ok(())
}

use tokio_postgres::Row;
use std::collections::{HashMap};
use serde::{Serialize,Deserialize};
use tokio_postgres::{Error};
use tracing::{info, debug};
use std::time::{SystemTime};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileSummary {
    handle: String,
    filename: String,
    filehash: String,
    calc: f64
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AccountSummary {
    handle: String,
    tx_type: String,
    account_name: String,
    calc: f64
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SecuritySummary {
    handle: String,
    tx_type: String,
    security_ticker: String,
    calc: f64
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Trade {
    pub id: Option<i32>,
    pub handle: String,
    pub filename: String,
    pub filehash: String,
    pub row: i32,
    pub account_name: String,
    pub account_number: String,
    pub security_description: String,
    pub security_ticker: String,
    pub asset_class: String,
    pub security_type: String,
    pub tx_type: String,
    pub cusip: String,
    pub price: f64,
    pub quantity: f64,
    pub commission: f64,
    pub fee: f64,
    pub principal: f64,
    pub net_amount: f64,
    pub trade_date: i64,
    pub settlement_date: i64,
    pub broker: String,     
    pub trader: String
}

impl From<Row> for Trade {
    fn from(row: tokio_postgres::Row) -> Self {
        Self {
            id: Some(row.get("id")),
            handle: row.get("handle"),
            filename: row.get("filename"),
            filehash: row.get("filehash"),
            row: row.get("row"),
            account_name: row.get("account_name"),
            account_number: row.get("account_number"),
            security_description: row.get("security_description"),
            security_ticker: row.get("security_ticker"),
            asset_class: row.get("asset_class"),
            security_type: row.get("security_type"),
            tx_type: row.get("tx_type"),
            cusip: row.get("cusip"),
            price: row.get("price"),
            quantity: row.get("quantity"),
            commission: row.get("commission"),
            fee: row.get("fee"),
            principal: row.get("principal"),
            net_amount: row.get("net_amount"),
            trade_date: row.get("trade_date"),
            settlement_date: row.get("settlement_date"),
            broker: row.get("broker"),
            trader: row.get("trader"),
        }
    }
}

pub async fn build_trades_table(client: &tokio_postgres::Client) -> Result<(), Error> {

    // Now we can execute a simple statement that just returns its parameter.
    client.query("CREATE TABLE trades (id SERIAL PRIMARY KEY,
        handle VARCHAR NOT NULL,
        filename VARCHAR NOT NULL,
        filehash VARCHAR NOT NULL,
        row INT NOT NULL,
        account_name VARCHAR NOT NULL,
        account_number VARCHAR NOT NULL,
        security_ticker VARCHAR NOT NULL,
        security_description VARCHAR NOT NULL,
        asset_class VARCHAR NOT NULL,
        security_type VARCHAR NOT NULL,
        tx_type VARCHAR NOT NULL,
        cusip VARCHAR NOT NULL,
        price FLOAT8 NOT NULL,
        quantity FLOAT8 NOT NULL,
        commission FLOAT8 NOT NULL,
        fee FLOAT8 NOT NULL,
        principal FLOAT8 NOT NULL,
        net_amount FLOAT8 NOT NULL,
        trade_date BIGINT NOT NULL,
        settlement_date BIGINT NOT NULL,
        broker VARCHAR NOT NULL,
        trader VARCHAR NOT NULL
        )", &[]).await?;

    Ok(())
}

pub async fn drop_trades_table(client: &tokio_postgres::Client) -> Result<(), Error> {

    // Now we can execute a simple statement that just returns its parameter.
    client.query("drop TABLE trades", &[]).await?;

    Ok(())
}


pub async fn get_all_trades(client: &tokio_postgres::Client, handle: &str) -> Result<Vec<Trade>, Error> {

    let rows = client.query("SELECT * FROM trades WHERE handle = $1", &[&handle]).await;
    let mut v: Vec<Trade> = Vec::new();

    match rows {
        Ok(r) => {
            for t in r.into_iter() {
                let trade: Trade = Trade::from(t);
                v.push(trade);
                debug!("pushed")
            }
        },
        Err(_) => debug!("{:?}", "wtf")
    }

    Ok(v)
}

async fn insert_file_summary(client: &tokio_postgres::Client, summary: &FileSummary) -> Result<(), Error> {

    info!("about to execute insert statement on alt pilot");
    let statement = client.prepare("INSERT INTO file_summaries (
        handle,
        filename,
        filehash,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

    info!("about to execute insert statement on alt pilot");
    client.execute(&statement,&[
        &summary.handle,
        &summary.filename, 
        &summary.filehash,
        &summary.calc,
        &SystemTime::now(),
        &SystemTime::now()
        ]).await?;
    Ok(())
}


async fn insert_account_summary(client: &tokio_postgres::Client, summary: &AccountSummary) -> Result<(), Error> {

    info!("about to execute insert statement on alt pilot");
    let statement = client.prepare("INSERT INTO account_summaries (
        handle,
        tx_type,
        account_name,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

    info!("about to execute insert statement on alt pilot");
    client.execute(&statement,&[
        &summary.handle,
        &summary.tx_type, 
        &summary.account_name,
        &summary.calc,
        &SystemTime::now(),
        &SystemTime::now()
        ]).await?;
    Ok(())
}


async fn insert_security_summary(client: &tokio_postgres::Client, summary: &SecuritySummary) -> Result<(), Error> {

    info!("about to execute insert statement on alt pilot");
    let statement = client.prepare("INSERT INTO security_summaries (
        handle,
        tx_type,
        security_ticker,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

    info!("about to execute insert statement on alt pilot");
    client.execute(&statement,&[
        &summary.handle,
        &summary.tx_type, 
        &summary.security_ticker,
        &summary.calc,
        &SystemTime::now(),
        &SystemTime::now()
        ]).await?;
    Ok(())
}

pub async fn summarize(client: &tokio_postgres::Client, alt_client: &tokio_postgres::Client, handle: &str) -> Result<(), Error> {

    for t in get_all_trades(client, handle).await?.into_iter() {
        info!("{:?}", t);
    }

    let values: Vec<(String,String)> = get_all_trades(client, handle).await?.into_iter().map(|x| (x.filename.clone(), x.filehash.clone()) ).rev().collect();
    let g = values.iter().fold(HashMap::new(), |mut acc, c| {
        *acc.entry((c.0.clone(),c.1.clone())).or_insert(0) += 1;
        acc
    });
    for k in g.keys() {
        let s = FileSummary {
            handle: handle.to_string(),
            filename: k.0.clone(),
            filehash: k.1.clone(),
            calc: g[k] as f64
        };
        insert_file_summary(&alt_client, &s).await?;
        info!("{:?}", s);
    }

    let values: Vec<(String,String, f64)> = get_all_trades(client, handle).await?.into_iter().map(|x| (x.tx_type.clone().to_uppercase(), x.account_name.clone().to_uppercase(), x.net_amount.abs()) ).rev().collect();
    let g = values.iter().fold(HashMap::new(), |mut acc, c| {
        *acc.entry((c.0.clone(),c.1.clone())).or_insert(c.2) += c.2;
        acc
    });

    println!("{:?}",g);

    for k in g.keys() {
        let s = AccountSummary {
            handle: handle.to_string(),
            tx_type: k.0.clone(),
            account_name: k.1.clone(),
            calc: g[k] as f64
        };
        insert_account_summary(&alt_client, &s).await?;
        info!("{:?}", s);
    }
 
 
    let values: Vec<(String,String, f64)> = get_all_trades(client, handle).await?.into_iter().map(|x| (x.tx_type.clone().to_uppercase(), x.security_ticker.clone().to_uppercase(), x.net_amount.abs()) ).rev().collect();
    let g = values.iter().fold(HashMap::new(), |mut acc, c| {
        *acc.entry((c.0.clone(),c.1.clone())).or_insert(c.2) += c.2;
        acc
    });

    for k in g.keys() {
        let s = SecuritySummary {
            handle: handle.to_string(),
            tx_type: k.0.clone(),
            security_ticker: k.1.clone(),
            calc: g[k] as f64
        };
        insert_security_summary(&alt_client, &s).await?;
        info!("{:?}", s);
    }

    Ok(())

}



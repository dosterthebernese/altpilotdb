use tokio_postgres::Row;
use serde::{Serialize,Deserialize};
use tokio_postgres::{Error};
use tracing::{info};

/// For add_item and query_item
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Trade {
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


pub async fn get_all_trades(client: &tokio_postgres::Client) -> Result<(), Error> {

    let rows = client.query("SELECT * FROM trades", &[]).await;

    match rows {
        Ok(r) => {
            for t in r.into_iter() {
                let trade: Trade = Trade::from(t);
                info!("{:?}", trade)
            }
        },
        Err(_) => info!("{:?}", "wtf")
    }

    Ok(())
}


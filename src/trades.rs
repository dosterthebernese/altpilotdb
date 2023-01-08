use serde::{Serialize,Deserialize};
use tokio_postgres::{Error};

/// For add_item and query_item
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Trade {
    pub filename: String,
    pub filehash: String,
    pub row: i32,
    pub account_name: String,
    pub account_number: String,
    pub securtiy_description: String,
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


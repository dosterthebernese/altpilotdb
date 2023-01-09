use bson::oid::ObjectId;
use chrono::NaiveDateTime;
use tokio_postgres::Row;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
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
pub struct TradeChain {
    pub head: Trade,
    pub chain: Vec<Trade>
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

impl Trade {
    pub fn is_chained(&self, other_trade: &Trade) -> bool {
        if self.security_ticker == other_trade.security_ticker &&
        self.trade_date <= other_trade.trade_date &&
        self.settlement_date > other_trade.trade_date  {
            true        
        } else {
            false
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

    let rows = client.query("SELECT * FROM trades WHERE handle = $1 ORDER BY id", &[&handle]).await;
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


async fn clean_chains(client: &tokio_postgres::Client, handle: &str) -> Result<(), Error> {

    let statement = client.prepare("delete from chains where handle = $1").await?;
    client.execute(&statement,&[&handle]).await?;

    Ok(())

}

async fn insert_chain(client: &tokio_postgres::Client, chain: &TradeChain) -> Result<(), Error> {

    let chain_id = ObjectId::new().to_string();


    let statement = client.prepare("INSERT INTO chains (
        handle,
        filename,
        filehash,
        row,
        chain_id,
        head,
        security_ticker,
        account_name,
        tx_type,
        price,
        quantity,
        commission,
        net_amount,
        broker,
        trade_date,
        settlement_date,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)").await?;

    info!("{:?}", &chain.head);

    client.execute(&statement,&[
        &chain.head.handle,
        &chain.head.filename,
        &chain.head.filehash,
        &chain.head.row,
        &chain_id,
        &true,
        &chain.head.security_ticker,
        &chain.head.account_name,
        &chain.head.tx_type,
        &chain.head.price.abs(),
        &chain.head.quantity.abs(),
        &chain.head.commission.abs(),
        &chain.head.net_amount.abs(),
        &chain.head.broker,
        &NaiveDateTime::from_timestamp_opt(chain.head.trade_date,0),
        &NaiveDateTime::from_timestamp_opt(chain.head.settlement_date,0),
        &SystemTime::now(),
        &SystemTime::now()
        ]).await?;



    for t in &chain.chain {


        let statement = client.prepare("INSERT INTO chains (
            handle,
            filename,
            filehash,
            row,
            chain_id,
            head,
            security_ticker,
            account_name,
            tx_type,
            price,
            quantity,
            commission,
            net_amount,
            broker,
            trade_date,
            settlement_date,
            inserted_at,
            updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)").await?;

        info!("{:?}", &chain.head);

        client.execute(&statement,&[
            &t.handle,
            &t.filename,
            &t.filehash,
            &t.row,
            &chain_id,
            &false,
            &t.security_ticker,
            &t.account_name,
            &t.tx_type,
            &t.price.abs(),
            &t.quantity.abs(),
            &t.commission.abs(),
            &t.net_amount.abs(),
            &t.broker,
            &NaiveDateTime::from_timestamp_opt(t.trade_date,0),
            &NaiveDateTime::from_timestamp_opt(t.settlement_date,0),
            &SystemTime::now(),
            &SystemTime::now()
            ]).await?;


    }


    Ok(())



}



async fn insert_file_summary(client: &tokio_postgres::Client, summary: &FileSummary) -> Result<(), Error> {

    let statement = client.prepare("INSERT INTO file_summaries (
        handle,
        filename,
        filehash,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

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

    let statement = client.prepare("INSERT INTO account_summaries (
        handle,
        tx_type,
        account_name,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

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

    let statement = client.prepare("INSERT INTO security_summaries (
        handle,
        tx_type,
        security_ticker,
        calc,
        inserted_at,
        updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)").await?;

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

pub async fn chain(client: &tokio_postgres::Client, alt_client: &tokio_postgres::Client, handle: &str) -> Result<(), Error> {

    info!("first I'll clean up for {:?}", handle);
    clean_chains(alt_client, handle).await?;

    let mut payload: Vec<TradeChain> = Vec::new();

    let mut already_in_a_chain: HashSet<i32> = HashSet::new();

    let all_trades = get_all_trades(client, handle).await?;
    for t in &all_trades {
        let mut ch: Vec<Trade> = Vec::new();
        for t2 in &all_trades {
            // we do this because if already in a chain, i don't need to make an inner chain
            if !already_in_a_chain.contains(&t2.id.unwrap()) {
                if t.is_chained(&t2) {
                    ch.push(t2.clone());
                    already_in_a_chain.insert(t2.id.unwrap());
                }                
            }
        }

        let tx_types: Vec<String> = ch.iter().map(|x| x.tx_type.clone()).collect();
        info!("{:?}", tx_types);
        let tx_types_u: Vec<_> = tx_types.into_iter().unique().collect();
        info!("{:?}", tx_types_u);

        if ch.len() > 0 && tx_types_u.len() > 1 { //there's distinct tx type greater than 2
            let tc = TradeChain {
                head: t.clone(),
                chain: ch
            };
            payload.push(tc);            
        }

    }

    for c in payload {
        // info!("HEAD {:?} {:?} {:?} {:?} {:?}", c.head.id, c.head.security_ticker, c.head.tx_type, c.head.trade_date, c.head.settlement_date);
        // for cm in c.chain {
        //     info!("MEMB {:?} {:?} {:?} {:?} {:?}", cm.id, cm.security_ticker, cm.tx_type, cm.trade_date, cm.settlement_date);
        // }
        insert_chain(alt_client, &c).await?;
    }

    Ok(())

}



use serde::{Serialize,Deserialize};

/// For add_item and query_item
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Trade {
    pub filename: String,
    pub filehash: String,
    pub row: usize,
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
    pub trade_date: String,
    pub settlement_date: String,
    pub broker: String, 
    pub trader: String
}

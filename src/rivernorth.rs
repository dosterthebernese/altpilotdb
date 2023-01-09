use crate::*;
use std::error::Error;

use itertools::Itertools;
use calamine::{Reader, open_workbook, Xlsx, DataType};
use chrono::{NaiveDateTime, NaiveDate, Duration as ChronoDuration};



pub fn get_header(h: Option<&DataType>) -> String{

	match h {
		Some(dt) => {
			match dt.get_string().expect("NEILISDUMB").to_lowercase().as_ref() {
				"portfolioaccountnumber" => "account_name".to_string(),
				"portfolioaccounttype" => "account_number".to_string(),
				"activity" => "tx_type".to_string(),
				"securitysymbol" => "security_ticker".to_string(),
				"cusip" => "cusip".to_string(),
				"securitydescription" => "security_description".to_string(),
				"tradedate" => "trade_date".to_string(),
				"quantity" => "quantity".to_string(),
				"principalunitcost" => "price".to_string(),
				"principal" => "principal".to_string(),
				"commission" => "commission".to_string(),
				"fee" => "fee".to_string(),
				"netamount" => "net_amount".to_string(),
				"settlementdate" => "settlement_date".to_string(),
				"securitytype" => "security_type".to_string(),
				"broker" => "broker".to_string(),
				"trader" => "trader".to_string(),
				_ => "nomatch".to_string()
			}
		},
		_ => {
			"NoBueno".to_string()
		}
	}

}

pub async fn insert_trade(client: &tokio_postgres::Client, trade: &trades::Trade) -> Result<(), Box<dyn Error>> {
    info!("{:?}, {:?}", client, trade);
    let statement = client.prepare("INSERT INTO trades (
    	handle,
    	filename,
    	filehash,
    	row,
    	account_name,
    	account_number,
    	security_description,
    	security_ticker,
    	asset_class,
    	security_type,
    	tx_type,
    	cusip,
    	price,
    	quantity,
    	commission,
    	fee,
    	principal,
    	net_amount,
    	trade_date,
    	settlement_date,
    	broker,
    	trader
    	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)").await?;

    client.execute(&statement,&[
    	&trade.handle,
    	&trade.filename, 
    	&trade.filehash,
    	&trade.row,
    	&trade.account_name,
    	&trade.account_number,
    	&trade.security_description,
    	&trade.security_ticker,
    	&trade.asset_class,
    	&trade.security_type,
    	&trade.tx_type,
    	&trade.cusip,
    	&trade.price,
    	&trade.quantity,
    	&trade.commission,
    	&trade.fee,
    	&trade.principal,
    	&trade.net_amount,
    	&trade.trade_date,
    	&trade.settlement_date,
    	&trade.broker,
    	&trade.trader
    	]).await?;
	Ok(())
}

pub async fn parse(client: &tokio_postgres::Client) -> Result<(), Box<dyn Error>> {
     let ifiles = vec!["/tmp/2019-09.xlsx","/tmp/2019-10.xlsx","/tmp/2019-11.xlsx"];

     for ifile in ifiles {
 
        let mut workbook: Xlsx<_> = open_workbook(ifile).expect("Cannot open file");
        // Read whole worksheet data and provide some statistics
        if let Some(Ok(range)) = workbook.worksheet_range("Sheet1") {
            let total_cells = range.get_size().0 * range.get_size().1;
            let non_empty_cells: usize = range.used_cells().count();
            println!("Found {} cells in 'Sheet1', including {} non empty cells",
                     total_cells, non_empty_cells);
            // alternatively, we can manually filter rows
            assert_eq!(non_empty_cells, range.rows()
                .flat_map(|r| r.iter().filter(|&c| c != &DataType::Empty)).count());

            let lens: Vec<usize> = range.rows().into_iter().map(|x| x.len()).rev().collect();
            let ulens: Vec<_> = lens.into_iter().unique().collect();
            assert_eq!(1,ulens.len());

            let idx_cap = ulens[0] as u32;

            let _original_headers: Vec<Option<&DataType>> = (0..idx_cap).collect::<Vec<u32>>().iter().map(|x| range.get_value((0,*x))).collect();        
            let mapped_headers: Vec<String> = (0..idx_cap).collect::<Vec<u32>>().iter().map(|x| rivernorth::get_header(range.get_value((0,*x)))).collect();
            let cusip_position = mapped_headers.iter().position(|x| x == "cusip");
            let account_name_position = mapped_headers.iter().position(|x| x == "account_name");
            let account_number_position = mapped_headers.iter().position(|x| x == "account_number");
            let security_description_position = mapped_headers.iter().position(|x| x == "security_description");
            let security_ticker_position = mapped_headers.iter().position(|x| x == "security_ticker");
            let security_type_position = mapped_headers.iter().position(|x| x == "security_type");
            let tx_type_position = mapped_headers.iter().position(|x| x == "tx_type");
            let price_position = mapped_headers.iter().position(|x| x == "price");
            let quantity_position = mapped_headers.iter().position(|x| x == "quantity");
            let commission_position = mapped_headers.iter().position(|x| x == "commission");
            let fee_position = mapped_headers.iter().position(|x| x == "fee");
            let principal_position = mapped_headers.iter().position(|x| x == "principal");
            let net_amount_position = mapped_headers.iter().position(|x| x == "net_amount");
            let trade_date_position = mapped_headers.iter().position(|x| x == "trade_date");
            let settlement_date_position = mapped_headers.iter().position(|x| x == "settlement_date");
            let broker_position = mapped_headers.iter().position(|x| x == "broker");
            let trader_position = mapped_headers.iter().position(|x| x == "trader");

            assert_eq!(false, mapped_headers.contains(&"nomatch".to_string()));


            for (i,r) in range.rows().enumerate() {
                if i == 0 {
                    continue
                }

                if let (
                    Some(cp), 
                    Some(anap), 
                    Some(anmp), 
                    Some(sdp), 
                    Some(stp), 
                    Some(sectypepos), 
                    Some(txtp), 
                    Some(pp), 
                    Some(qtyp),
                    Some(cmmp),
                    Some(feep),
                    Some(princep),
                    Some(nap),
                    Some(trdp), 
                    Some(stdp), 
                    Some(brkp), 
                    Some(trap)) = (
                    cusip_position, 
                    account_name_position, 
                    account_number_position, 
                    security_description_position, 
                    security_ticker_position, 
                    security_type_position,
                    tx_type_position, 
                    price_position,
                    quantity_position,
                    commission_position,
                    fee_position,
                    principal_position,
                    net_amount_position, 
                    trade_date_position, 
                    settlement_date_position, 
                    broker_position, 
                    trader_position) {

                    let string_trade_date = r[trdp].to_string();
                    // this is excel bullshit number of days since Jan 1 1900
                    let istring_trade_date = string_trade_date.parse::<i64>().unwrap_or(1);
                    // Caution! Excel dates after 28th February 1900 are actually one day out. Excel behaves as though the date 29th February 1900 existed, which it didn't.
                    let string_settlement_date = r[stdp].to_string();
                    // this is excel bullshit number of days since Jan 1 1900
                    let istring_settlement_date = string_settlement_date.parse::<i64>().unwrap_or(1);
                    // Caution! Excel dates after 28th February 1900 are actually one day out. Excel behaves as though the date 29th February 1900 existed, which it didn't.
                    // river north gives dates, not times, so setting to market close (closed end funds)
                    let excel_bullshit: NaiveDateTime = NaiveDate::from_ymd_opt(1899, 12, 30).unwrap().and_hms_opt(16, 0, 0).unwrap();
                    let this_bullshit_trade_date = excel_bullshit + ChronoDuration::days(istring_trade_date);
                    let this_bullshit_settlment_date = excel_bullshit + ChronoDuration::days(istring_settlement_date);

                    let trade = trades::Trade {
                    	id: None,
                    	handle: "rivernorth".to_string(),
                    	filename: ifile.to_string(),
                    	filehash: utils::sha_fmt(ifile).unwrap_or("failedhash".to_string()),
                    	row: i as i32,
                        account_name: r[anap].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        account_number: r[anmp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        security_description: r[sdp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        security_ticker: r[stp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        security_type: r[sectypepos].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        asset_class: r[sectypepos].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        tx_type: r[txtp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        broker: r[brkp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        trader: r[trap].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        cusip: r[cp].get_string().unwrap_or("ALTP ERROR NO DATA PROVIDED").to_string(),
                        price: r[pp].get_float().unwrap_or(0.),
                        quantity: r[qtyp].get_float().unwrap_or(0.),
                        commission: r[cmmp].get_float().unwrap_or(0.),
                        fee: r[feep].get_float().unwrap_or(0.),
                        principal: r[princep].get_float().unwrap_or(0.),
                        net_amount: r[nap].get_float().unwrap_or(0.),
                        trade_date: this_bullshit_trade_date.timestamp(),
                        settlement_date: this_bullshit_settlment_date.timestamp(),
                    };

                    info!("{:?}", trade);
                    insert_trade(&client, &trade).await?;

                }
            }
        }
    }

	Ok(())
}
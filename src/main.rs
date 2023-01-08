mod rivernorth;
mod trades;
mod utils;
use clap::Parser;
use tokio_postgres::{NoTls, Error};

//use std::error::Error;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

async fn get_client() -> Result<tokio_postgres::Client, Error> {
   // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=tradellama password='puppyjuice06!'", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

async fn build_trades_table(client: &tokio_postgres::Client) -> Result<(), Error> {

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
        price FLOAT8 NOT NULL,
        quantity FLOAT8 NOT NULL,
        commission FLOAT8 NOT NULL,
        broker VARCHAR NOT NULL,
        trader VARCHAR NOT NULL,
        trade_date TIMESTAMPTZ NOT NULL,
        settlement_date TIMESTAMPTZ NOT NULL
        )", &[]).await?;

    Ok(())
}

async fn drop_trades_table(client: &tokio_postgres::Client) -> Result<(), Error> {

    // Now we can execute a simple statement that just returns its parameter.
    client.query("drop TABLE trades", &[]).await?;

    Ok(())
}


// async fn postgres_stuff() -> Result<(), Error> {

//     let client = get_client().await.unwrap();
//     // Now we can execute a simple statement that just returns its parameter.
//     let rows = client
//         .query("SELECT $1::TEXT", &[&"hello world"])
//         .await?;

//     // And then check that we got back the same string we sent over.
//     let value: &str = rows[0].get(0);
//     assert_eq!(value, "hello world");
//     Ok(())

// }


/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
   /// Name of the Function to Run
   #[arg(short, long)]
   name: String,

   /// Number of times to greet
   #[arg(short, long, default_value_t = 1)]
   count: u8,
}


#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
pub async fn main() {

//    postgres_stuff().await.unwrap();

    let client = get_client().await.unwrap();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let args = Args::parse();
    info!(args.count, "Preparing to see what you passed me in the count of args: ");        
    for _ in 0..args.count {
        info!(args.name, "You passed: ");
        match args.name.as_ref() {
            "build" => {
                match &build_trades_table(&client).await {
                    Ok(_) => info!("I built the trades table."),
                    Err(err) => error!("I failed to build the trades table.  The reason as per postgres is\n: {:?}\n\n", err),

                }
            },
            "drop" => {
                match &drop_trades_table(&client).await {
                    Ok(_) => info!("I dropped the trades table."),
                    Err(err) => error!("I failed to drop the trades table.  The reason as per postgres is\n: {:?}\n\n", err),

                }
            },
            "parse" => {
                match &rivernorth::parse(&client).await {
                    Ok(_) => info!("I parsed the usual river north files."),
                    Err(err) => error!("I failed to pare the usual river north files.  The reason as per river north's parser is\n: {:?}\n\n", err),

                }
            },


            _ => info!("I have no idea what you're telling me to do.")
        }
    }

 }


#[cfg(test)]
mod tests {

//    use super::*;

     
    ///probably should not drop create but for dev wtf
    #[tokio::test]
    async fn can_connect_local_and_drop_and_create_trades_table() {
        assert_eq!(1, 1);
    }

}
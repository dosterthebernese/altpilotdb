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

async fn get_alt_local_client() -> Result<tokio_postgres::Client, Error> {
   // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password='postgres' dbname='altpilot_dev'", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
            println!("connection error: {}", e);
        }
    });
    Ok(client)
}

async fn get_alt_remote_client() -> Result<tokio_postgres::Client, Error> {


//postgres://green_feather_3408:czMSpiovqNTgssS@top2.nearest.of.green-feather-3408-db.internal:5432/green_feather_3408?sslmode=disable

   // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=localhost port=15432 user=tradellama password='puppyjuice06!' dbname='green_feather_3408'", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
            println!("connection error: {}", e);
        }
    });
    Ok(client)
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
                match &trades::build_trades_table(&client).await {
                    Ok(_) => info!("I built the trades table."),
                    Err(err) => error!("I failed to build the trades table.  The reason as per postgres is\n: {:?}\n\n", err),

                }
            },
            "drop" => {
                match &trades::drop_trades_table(&client).await {
                    Ok(_) => info!("I dropped the trades table."),
                    Err(err) => error!("I failed to drop the trades table.  The reason as per postgres is\n: {:?}\n\n", err),

                }
            },
            "parsern" => {
                match &rivernorth::parse(&client).await {
                    Ok(_) => info!("I parsed the usual river north files."),
                    Err(err) => error!("I failed to pare the usual river north files.  The reason as per river north's parser is\n: {:?}\n\n", err),

                }
            },
            "summarizern" => {
               let alt_client = get_alt_remote_client().await.unwrap();
               //let alt_client = get_alt_local_client().await.unwrap();

                match &trades::summarize(&client,&alt_client,"rivernorth").await {
                    Ok(_) => info!("I fetched the trades table for rn."),
                    Err(err) => error!("I failed to fetch the trades table for rn.  The reason as per postgres is\n: {:?}\n\n", err),
                }
            },


            _ => info!("I have no idea what you're telling me to do.")
        }
    }

 }


#[cfg(test)]
mod tests {

    use super::*;

     
    ///probably should not drop create but for dev wtf
    #[tokio::test]
    async fn can_connect_locals() {
 
        let client = get_client().await.unwrap();
        let alt_client = get_alt_local_client().await.unwrap();
        let alt_remote_client = get_alt_remote_client().await.unwrap();
        println!("{:?}", client);
        println!("{:?}", alt_client);
        println!("{:?}", alt_remote_client);
        assert_eq!(1, 1);
 
        let rows = alt_client.query("SELECT email FROM users", &[]).await.unwrap();
        println!("{:?}", rows);

        let rows = alt_remote_client.query("SELECT filename FROM file_summaries", &[]).await.unwrap();
        println!("{:?}", rows);


    }

}
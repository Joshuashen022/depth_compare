pub mod deep;
mod connection;

use connection::{BinanceSpotOrderBook};
// use deep::Event;
// use tokio_tungstenite::connect_async;
// use url::Url;
// use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
// use futures_util::StreamExt;
use anyhow::Result;
use tokio::sync::Mutex;
use std::sync::Arc;
// use tokio::spawn;

#[tokio::main]
async fn main() -> Result<()> {

    let order_book_depth = BinanceSpotOrderBook::new();
    let order_book_level_depth = BinanceSpotOrderBook::new();

    // Start depth order book
    match order_book_depth.depth(){
        Ok(_) => (),
        Err(e) => println!("{}",e),
    };

    // Start depth level order book
    order_book_level_depth.level_depth();

    loop{
        let _depth = order_book_depth.get_snapshot().await;
        let depth_level = order_book_level_depth.get_snapshot().await;
        if depth_level.is_some(){
            println!("{:?}", depth_level.unwrap());
        }

        println!();
        println!();
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())

}

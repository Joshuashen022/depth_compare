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
    let mut rx1 = order_book_depth.depth().unwrap();

    // Start depth level order book
    let mut rx2 = order_book_level_depth.level_depth().unwrap();
    tokio::spawn(async move {
        while let Some(message) = rx1.recv().await{
            println!("receive1 {}", message.last_update_id);
        }
    });

    tokio::spawn(async move {
        while let Some(message) = rx2.recv().await{
            println!("receive2 {}", message.last_update_id);
        }
    });
    

    loop{
        println!();
        println!();
        sleep(Duration::from_secs(1)).await;
        let depth = order_book_depth.snapshot().await;

        let depth_level = order_book_level_depth.snapshot().await;
        if depth_level.is_none() || depth.is_none(){
            println!("depth_level {}, depth {}", depth_level.is_none(), depth.is_none());
            continue
        }
        //
        let depth = depth.unwrap();
        let depth_level = depth_level.unwrap();
        let depth_time = depth.time_stamp;
        let depth_level_time = depth_level.time_stamp;
        let contains = depth.if_contains(&depth_level);

        println!("{} {}, contains? {}", depth_time, depth_level_time, contains);

        if !contains {
            let (different_bids, different_asks ) = depth.find_different(&depth_level);
            println!("bids different {}", different_bids.len());
            // println!("{:?}", different_bids);
            println!("asks different {}", different_asks.len());
            // println!("{:?}", different_asks);
        }

    }

    Ok(())

}

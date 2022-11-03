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

    let order_book = BinanceSpotOrderBook::new();

    order_book.depth();

    // In case thread is out of control
    let mut default_break = 0;

    loop{
        // if let Some(snapshot) = order_book.get_snapshot().await{
        //     println!("snapshot id: {}, ask cnt: {}, bid cnt: {}", snapshot.last_update_id, snapshot.asks.len(), snapshot.bids.len());
        //
        //     println!("------ asks ------");
        //     for ask in snapshot.asks.iter().take(5) {
        //         println!("price: {}, amount: {}", ask.price, ask.amount);
        //     }
        //
        //     println!("------ bids ------");
        //     for bid in snapshot.bids.iter().take(5) {
        //         println!("price: {}, amount: {}", bid.price, bid.amount);
        //     }
        //
        //     if default_break > 20 {
        //         break;
        //     }
        //     default_break += 1;
        // };

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())

}

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
        println!();
        println!();
        sleep(Duration::from_secs(1)).await;
        let depth = order_book_depth.get_snapshot().await;

        let depth_level = order_book_level_depth.get_snapshot().await;
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

    // Ok(())

}

#[test]
fn read_and_compare()-> Result<()>{

    use std::fs::OpenOptions;
    use std::io::Read;
    use crate::deep::BinanceSpotOrderBookSnapshot;

    let mut reader1 = OpenOptions::new()
        .read(true).open("depth.cache")?;
    let mut reader2 = OpenOptions::new()
        .read(true).open("depth_level.cache")?;

    let mut buffer1 = String::new();
    let mut buffer2 = String::new();

    reader1.read_to_string(&mut buffer1)?;
    reader2.read_to_string(&mut buffer2)?;

    buffer1.pop();
    buffer2.pop();

    let depths:Vec<BinanceSpotOrderBookSnapshot> = buffer1.split("\n").collect::<Vec<_>>().iter()
        .map(|s|BinanceSpotOrderBookSnapshot::from_string(s.to_string()))
        .collect();
    let depth_levels:Vec<BinanceSpotOrderBookSnapshot> = buffer2.split("\n").collect::<Vec<_>>().iter()
        .map(|s|BinanceSpotOrderBookSnapshot::from_string(s.to_string()))
        .collect();
    println!("depths {}, depth_levels {}", depths.len(), depth_levels.len());
    let mut contains = false;
    let mut satisfy_queue = Vec::new();
    let mut counter = 0;
    for depth in depths{
        for depth_level in &depth_levels{
            let 结果 = depth.if_contains(depth_level);
            let level_a_len = depth_level.asks.len();
            let level_b_len = depth_level.bids.len();
            let (different_bids, different_asks ) = depth.find_different(&depth_level);

            let depth_time = depth.time_stamp;
            let depth_id = depth.last_update_id;

            let dl_time = depth_level.time_stamp;
            let dl_id = depth_level.last_update_id;
            // println!(" Depth {}-{} Depth Level {}-{} {}", depth_time, depth_id, dl_time ,dl_id , 结果);

            // println!("Time {} Id {} {}", depth_time - dl_time, depth_id - dl_id, 结果);
            // println!("different bids {} asks {}", different_bids.len(), different_asks.len());
            // println!("bids {} asks {}", level_b_len, level_a_len);
            if 结果 {
                let result = format!(" Depth {}-{} Depth Level {}-{} {}", depth_time, depth_id, dl_time ,dl_id , 结果);
                satisfy_queue.push(result);
                contains = true;
                counter += 1;
            }
        }
    }
    println!("done {}", contains );
    for res in satisfy_queue{
        println!("done {}", res );
    }

    println!("{}", counter);

    Ok(())
}



#[test]
fn read_and_compare2()-> Result<()>{

    use std::fs::OpenOptions;
    use std::io::Read;
    use std::io::prelude::*;
    use crate::deep::BinanceSpotOrderBookSnapshot;
    let mut reader1 = OpenOptions::new()
        .read(true).open("depth.cache")?;
    let mut reader2 = OpenOptions::new()
        .read(true).open("depth_level.cache")?;
    let mut file = OpenOptions::new();
    let mut reader = file.create(true).write(true).open("results").unwrap();

    let mut buffer1 = String::new();
    let mut buffer2 = String::new();

    reader1.read_to_string(&mut buffer1)?;
    reader2.read_to_string(&mut buffer2)?;

    buffer1.pop();
    buffer2.pop();

    let depths:Vec<BinanceSpotOrderBookSnapshot> = buffer1.split("\n").collect::<Vec<_>>().iter()
        .map(|s|BinanceSpotOrderBookSnapshot::from_string(s.to_string()))
        .collect();
    let depth_levels:Vec<BinanceSpotOrderBookSnapshot> = buffer2.split("\n").collect::<Vec<_>>().iter()
        .map(|s|BinanceSpotOrderBookSnapshot::from_string(s.to_string()))
        .collect();
    
    let message = format!("depths {}, depth_levels {} \n", depths.len(), depth_levels.len());
    reader.write_all(message.as_bytes()).unwrap_or(());

    let mut res_queue = Vec::new();
    for depth_level in &depth_levels{
        
        let mut matchs_depths = Vec::new();
        let mut differents_len = 200;
        let mut different = Vec::new();
        // let mut compare = false;
        for depth in &depths{
            let 结果 = depth.if_contains(depth_level);
            // compare = 结果;
            let depth_time = depth.time_stamp;
            let depth_id = depth.last_update_id;
            // println!(" Depth {}-{} Depth Level {}-{} {}", depth_time, depth_id, dl_time ,dl_id , 结果);

            // println!("Time {} Id {} {}", depth_time - dl_time, depth_id - dl_id, 结果);
            // println!("different bids {} asks {}", different_bids.len(), different_asks.len());
            // println!("bids {} asks {}", level_b_len, level_a_len);
            if 结果 {
                matchs_depths.push(depth.clone());
            } else {
                let (bid, ask) = depth.find_different(depth_level);
                if (bid.len() + ask.len()) < differents_len {
                    differents_len = bid.len() + ask.len();
                    different = vec![(bid, ask, depth_id, depth_time)];
                }
            }
        }
        let dl_time = depth_level.time_stamp;
        let dl_id = depth_level.last_update_id;
        let length = matchs_depths.len();
        let mut res = format!("{} {} {} matches: ", dl_time, dl_id, matchs_depths.len());
        for m in matchs_depths {
            res += &format!("{} ", m.last_update_id);
        }
        if length == 0 {
            for (bid, ask, depth_id, depth_time) in different {
                res += &format!("depth_id {} depth_time {}, bid {}, ask {}", depth_id, depth_time, bid.len(), ask.len());
            }
        }

        res_queue.push(res);
    }
    
    for raw in res_queue {
        let raw = format!("{}\n", raw);
        reader.write_all(raw.as_bytes()).unwrap_or(());
        
    }
    

    Ok(())
}


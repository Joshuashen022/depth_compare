use std::collections::VecDeque;
use crate::deep::{Event, BinanceSpotOrderBookSnapshot, Shared};
use tokio_tungstenite::connect_async;
use url::Url;
// use tokio::time::{sleep, Duration};
use futures_util::StreamExt;
use anyhow::{Result, Error};
use anyhow::{bail, anyhow};
use tokio::sync::Mutex;
// use tokio::select;
use std::sync::{Arc, RwLock};
use futures_util::future::err;
// use tokio::spawn;

const DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth@100ms";
const LEVEL_DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth20@100ms";
const REST: &str = "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000";
const MAX_BUFFER: usize = 5;

pub struct BinanceSpotOrderBook {
    status: Arc<Mutex<bool>>,
    shared: Arc<RwLock<Shared>>,
}

impl BinanceSpotOrderBook {

    pub fn new() -> Self {
        BinanceSpotOrderBook {
            status: Arc::new(Mutex::new(false)),
            shared: Arc::new(RwLock::new(Shared::new()))
        }
    }

    // acquire a order book with "depth method"
    pub fn depth(&self) {
        let shared = self.shared.clone();
        let status = self.status.clone();
        tokio::spawn(async move {
            let mut default_exit = 0;
            loop {

                let res : Result<()> = {
                    let url = Url::parse(DEPTH_URL).expect("Bad URL");

                    let res = connect_async(url).await;
                    let mut stream = match res{
                        Ok((stream, _)) => stream,
                        Err(_) => bail!("connect_async url error"),
                    };
                    println!(" Connected to wss://Depth ");
                    let snapshot: BinanceSpotOrderBookSnapshot = reqwest::get(REST)
                        .await?
                        .json()
                        .await?;
                    println!(" Connected to https://REST ");
                    let mut buffer = VecDeque::new();

                    while let Ok(msg) = stream.next().await.unwrap(){ //
                        if !msg.is_text() {
                            continue
                        }
                        let text = msg.into_text().unwrap();
                        let event: Event = serde_json::from_str(&text)?;

                        buffer.push_back(event);

                        if buffer.len() == MAX_BUFFER {
                            break
                        }
                    };

                    println!(" Collected event ");
                    let mut overbook_setup = false;

                    while let Some(event) = buffer.pop_front() {
                        println!(" Check event ");
                        if event.first_update_id > snapshot.last_update_id {
                            continue;
                        }
                        println!(" event is usable ");
                        if event.match_snapshot(snapshot.last_update_id) {
                            println!(" Found match snapshot ");
                            let mut orderbook = shared.write().unwrap();
                            orderbook.load_snapshot(&snapshot);
                            orderbook.add_event(event);

                            overbook_setup = true;
                            break;
                        } else {
                            println!(" Not match ");
                        }

                    }

                    println!(" Done Checking");
                    let mut counter = 0;
                    if !overbook_setup {

                        while let Ok(msg) = stream.next().await.unwrap(){ //
                            counter += 1;
                            if !msg.is_text() {
                                continue
                            }
                            let text = msg.into_text().unwrap();
                            let event: Event = serde_json::from_str(&text)?;
                            if event.first_update_id > snapshot.last_update_id {
                                println!(" event is far ahead ");
                                break;
                            }
                            if event.match_snapshot(snapshot.last_update_id) {
                                println!(" Found match snapshot ");
                                let mut orderbook = shared.write().unwrap();
                                orderbook.load_snapshot(&snapshot);
                                orderbook.add_event(event);

                                overbook_setup = true;
                                break;
                            } else {
                                println!(" Not match should {} actually {}-{}",
                                         snapshot.last_update_id,
                                         event.first_update_id,
                                         event.last_update_id
                                );
                            }
                        };

                        continue
                    }

                    if !overbook_setup{
                        continue
                    }
                    println!(" Done Checking 2 {}", counter);
                    // {
                    //     let mut guard  = status.lock().await;
                    //     *guard = true;
                    // }

                    while let Ok(msg) = stream.next().await.unwrap(){
                        if !msg.is_text() {
                            continue
                        }
                        let text = msg.into_text().unwrap();
                        let event: Event = serde_json::from_str(&text)?;
                        let mut orderbook = shared.write().unwrap(); // Acquire guard <orderbook>

                        let event_id = event.first_update_id;
                        match orderbook.update_snapshot(event){
                            Ok(()) => println!("Update success"),
                            Err(_) => {
                                if event_id < orderbook.id() + 1 {
                                    // Orderook is ahead of event, so we wait for next event
                                    continue
                                } else {
                                    println!("Update snapshot failed, should {} actually {}",
                                             orderbook.id() + 1, event_id,
                                    );
                                    break
                                }
                            }
                        }
                    };

                    Err(anyhow!("Error happened, start from top"))
                };
                match res {
                    Ok(_) => println!("Finish all code"),
                    Err(e) => println!("Error happen when running code: {:?}", e),
                }

                if default_exit > 20 {
                    println!("Using default break");
                    break
                }

                default_exit += 1;
            }
            Ok::<(), Error>(())
        });

    }

    // acquire a order book with "level depth method"
    pub async fn level_depth(&self){

    }

    pub async fn get_snapshot(&self) -> Option<BinanceSpotOrderBookSnapshot>{
        let mut current_status = false;

        {
            let status = self.status.clone();
            let status_guard = status.lock().await;
            current_status = (*status_guard).clone();

        }// Release the guard immediately


        if current_status{
            Some(self.shared.write().unwrap().get_snapshot())
        } else{
            None
        }

    }
}





use std::collections::VecDeque;
use crate::deep::{Event, BinanceSpotOrderBookSnapshot, Shared};
use tokio_tungstenite::connect_async;
use url::Url;
use tokio::time::{sleep, Duration};
use futures_util::StreamExt;
use anyhow::{Result, Error};
use anyhow::{bail, anyhow};
use tokio::sync::Mutex;
// use tokio::select;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use futures_util::future::err;
// use tokio::spawn;

const DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth@100ms";
const LEVEL_DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth20@100ms";
const REST: &str = "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000";
const MAX_BUFFER: usize = 30;

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
    pub fn depth(&self) -> Result<()> {
        let shared = self.shared.clone();
        let status = self.status.clone();
        let buffer = Arc::new(Mutex::new(VecDeque::<Event>::new()));
        let buffer_clone1 = buffer.clone();

        // Thread to maintain buffer from stream
        tokio::spawn(async move {
            println!("Start buffer maintain thread");
            loop{
                let url = Url::parse(DEPTH_URL).expect("Bad URL");

                let res = connect_async(url).await;
                let mut stream = match res{
                    Ok((stream, _)) => stream,
                    Err(e) => return anyhow!("{:?}", e),
                };

                while let Ok(msg) = stream.next().await.unwrap(){ //
                    if !msg.is_text() {
                        continue
                    }

                    let text = match msg.into_text(){
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    let event: Event = match serde_json::from_str(&text){
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    let mut guard = buffer_clone1.lock().await;

                    if (*guard).len() == MAX_BUFFER {
                        let _ = (*guard).pop_front();
                        (*guard).push_back(event);
                    } else {
                        (*guard).push_back(event);
                    }
                };
            }
        });

        let buffer_clone2 = buffer.clone();

        tokio::spawn(async move{
            let mut default_exit = 0;
            println!("Start OrderBook thread");
            loop {
                let res : Result<()> = {
                    // Wait for a while to collect event into buffer
                    sleep(Duration::from_millis(1000)).await;

                    let snapshot: BinanceSpotOrderBookSnapshot = reqwest::get(REST)
                        .await?
                        .json()
                        .await?;
                    sleep(Duration::from_millis(500)).await;
                    let mut buffer = VecDeque::<Event>::new();
                    {
                        let mut guard = buffer_clone2.lock().await;
                       buffer.append(&mut (*guard));
                    }
                    println!("Buffer len {}", buffer.len());
                    println!("Snap shot {}", snapshot.last_update_id); // 2861806778
                    let mut overbook_setup = false;
                    while let Some(event) = buffer.pop_front() {
                        println!(" Event {}-{}", event.first_update_id, event.last_update_id);
                        // Event 2861806779-2861806780

                        if snapshot.last_update_id >= event.first_update_id  {
                            // step 4
                            continue
                        }

                        if event.match_snapshot(snapshot.last_update_id) {
                            println!(" Found match snapshot ");
                            let mut orderbook = shared.write().unwrap();
                            orderbook.load_snapshot(&snapshot);
                            orderbook.add_event(event);

                            overbook_setup = true;
                            break;
                        } else {
                            // println!(" No match ");
                        }

                        if event.first_update_id > snapshot.last_update_id + 1 {
                            println!("Rest event is not usable, need a new snap shot ");
                            println!();
                            break;
                        }

                    }

                    if overbook_setup {
                        // Overbook initialize success

                        loop {
                            {
                                let mut guard = buffer_clone2.lock().await;
                                buffer.append(&mut (*guard));// TODO::not sure about time costing
                            }
                            println!("Buffer2 len {}", buffer.len());
                            // Sleep for a while to collect event by another thread
                            sleep(Duration::from_millis(1000)).await;

                            let instance = Instant::now();
                            let buffer_len = buffer.len();
                            let mut need_new_snap_snot = false;

                            // Acquire guard <orderbook>
                            let mut orderbook = shared.write().unwrap();


                            while let Some(event) = buffer.pop_front() {
                                println!("order book {}, Event {}-{}",
                                         orderbook.id(), event.first_update_id, event.last_update_id);

                                if event.first_update_id > orderbook.id() + 1 {
                                    println!("All event is not usable, need a new snap shot ");
                                    need_new_snap_snot = true;
                                    break;
                                } else if event.first_update_id == orderbook.id() + 1 {
                                    println!("Update complete");
                                    orderbook.add_event(event)
                                } else {
                                    continue
                                }

                            }

                            if need_new_snap_snot {
                                break;
                            }

                            // println!("deal {} Event used {}ms", buffer_len, instance.elapsed().as_millis());
                            // This step cost less than 1ms
                        }
                    }

                    Ok(())
                };

                match res {
                    Ok(_) => (),
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

        Ok(())
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





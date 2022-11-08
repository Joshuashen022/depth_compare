use std::collections::VecDeque;
use crate::deep::{LevelEvent, Event, BinanceSpotOrderBookSnapshot, Shared, BinanceSnapshot};
use tokio_tungstenite::{connect_async, tungstenite};
use tungstenite::Message;
use url::Url;
use tokio::time::{sleep, Duration};
use futures_util::StreamExt;
use anyhow::{Result, Error};
use anyhow::{bail, anyhow};
use tokio::sync::Mutex;
// use tokio::select;
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use futures_util::future::err;
use tracing::{error, info, trace};

const DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth@100ms";
const LEVEL_DEPTH_URL: &str = "wss://stream.binance.com:9443/ws/bnbbtc@depth20@100ms";
const REST: &str = "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000";
const MAX_BUFFER: usize = 30;
const MAX_BUFFER_EVENTS: usize = 5;

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

    /// acquire a order book with "depth method"
    pub fn depth(&self) -> Result<()> {
        let shared = self.shared.clone();
        let status = self.status.clone();
        // Thread to maintain Order Book
        let _ = tokio::spawn(async move{
            let mut default_exit = 0;
            info!("Start OrderBook thread");
            loop {
                let res : Result<()> = {
                    {
                        let mut guard = status.lock().await;
                        (*guard) = false;
                    }

                    let url = Url::parse(DEPTH_URL).expect("Bad URL");

                    let res = connect_async(url).await;
                    let mut stream = match res{
                        Ok((stream, _)) => stream,
                        Err(e) => {
                            default_exit += 1;
                            error!("Error calling {}, {:?}",DEPTH_URL, e);
                            continue
                        },
                    };

                    let mut buffer_events = VecDeque::new();
                    while let Ok(message) = stream.next().await.unwrap(){ //
                        let event = deserialize_message(message);
                        if event.is_none(){
                            continue
                        }
                        let event = event.unwrap();

                        buffer_events.push_back(event);

                        if buffer_events.len() == MAX_BUFFER_EVENTS{
                            break
                        }
                    };

                    // Wait for a while to collect event into buffer
                    trace!("Calling Https://");
                    let snapshot: BinanceSnapshot = reqwest::get(REST)
                        .await?
                        .json()
                        .await?;
                    trace!("Done Calling Https://");

                    trace!("Snap shot {}", snapshot.last_update_id); // 2861806778
                    let mut overbook_setup = false;
                    while let Some(event) = buffer_events.pop_front() {
                        trace!(" Event {}-{}", event.first_update_id, event.last_update_id);

                        if snapshot.last_update_id >= event.last_update_id  {
                            continue
                        }

                        if event.match_snapshot(snapshot.last_update_id) {
                            info!(" Found match snapshot 1");
                            let mut orderbook = shared.write().unwrap();
                            orderbook.load_snapshot(&snapshot);
                            orderbook.add_event(event);

                            overbook_setup = true;

                            break;
                        } else {
                            // println!(" No match ");
                        }

                        if event.first_update_id > snapshot.last_update_id + 1 {
                            error!("Rest event is not usable, need a new snap shot ");

                            break;
                        }

                    }

                    if overbook_setup {

                        while let Some(event) = buffer_events.pop_front()  {
                            let mut orderbook = shared.write().unwrap();
                            orderbook.add_event(event);
                        }

                    } else {

                        while let Ok(message) = stream.next().await.unwrap() {

                            let event = deserialize_message(message);
                            if event.is_none(){
                                continue
                            }
                            let event = event.unwrap();


                            trace!(" Event {}-{}", event.first_update_id, event.last_update_id);

                            // [E.U,..,E.u] S.u
                            if snapshot.last_update_id >= event.last_update_id  {
                                continue
                            }

                            let mut orderbook = shared.write().unwrap();
                            // [E.U,..,S.u,..,E.u]
                            if event.match_snapshot(snapshot.last_update_id) {
                                info!(" Found match snapshot 2");

                                orderbook.load_snapshot(&snapshot);
                                orderbook.add_event(event);

                                overbook_setup = true;
                                break;
                            }

                            // S.u [E.U,..,E.u]
                            if event.first_update_id > snapshot.last_update_id + 1 {
                                error!("Rest event is not usable, need a new snap shot ");

                                break;
                            }

                            if event.first_update_id > orderbook.id() + 1 {
                                error!("All event is not usable, need a new snap shot ");
                                error!("order book {}, Event {}-{}",
                                         orderbook.id(), event.first_update_id, event.last_update_id);

                                break;
                            }
                        }

                    }


                    if overbook_setup {
                        let mut guard = status.lock().await;
                        (*guard) = true;
                    } else {

                        continue
                    }

                    info!(" Overbook initialize success, now keep listening ");
                    // Overbook initialize success
                    while let Ok(message) = stream.next().await.unwrap() {
                        let event = deserialize_message(message);
                        if event.is_none(){
                            continue
                        }
                        let event = event.unwrap();

                        let mut orderbook = shared.write().unwrap();
                        if event.first_update_id > orderbook.id() + 1 {
                            error!("All event is not usable, need a new snap shot ");
                            error!("order book {}, Event {}-{}",
                                     orderbook.id(), event.first_update_id, event.last_update_id);

                            break;
                        } else if event.first_update_id == orderbook.id() + 1 {
                            let f_id = event.first_update_id;
                            let l_id = event.last_update_id;
                            orderbook.add_event(event);
                            println!("After add event {}, {} {}", orderbook.id(), f_id, l_id);
                        }

                    }

                    Ok(())
                };

                match res {
                    Ok(_) => (),
                    Err(e) => error!("Error happen when running code: {:?}", e),
                }

                if default_exit > 20 {
                    error!("Using default break");
                    break
                }

                default_exit += 1;
            }
            Ok::<(), Error>(())
        });

        Ok(())
    }

    pub fn level_depth(&self) {
        let shared = self.shared.clone();

        // This is not actually used
        let status = self.status.clone();


        let _ = tokio::spawn(async move {
            println!("Start Level Buffer maintain thread");
            loop{
                let url = Url::parse(LEVEL_DEPTH_URL).expect("Bad URL");

                let res = connect_async(url).await;
                let mut stream = match res{
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        println!("Error {:?}, reconnecting {}", e, LEVEL_DEPTH_URL);
                        continue
                    },
                };

                {
                    let mut guard = status.lock().await;
                    (*guard) = true;
                }

                use std::time::{UNIX_EPOCH, SystemTime};

                while let Ok(msg) = stream.next().await.unwrap(){ //
                    if !msg.is_text() {
                        continue
                    }

                    let text = match msg.into_text(){
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    let level_event: LevelEvent = match serde_json::from_str(&text){
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    if let Ok(mut guard) = shared.write(){
                        (*guard).set_level_event(level_event, time.as_millis() as i64);
                    }
                };
            }

        });
    }

    /// Get the snapshot of the current Order Book
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

fn deserialize_message(message: Message) -> Option<Event>{
    if !message.is_text() {
        return None
    }

    let text = match message.into_text(){
        Ok(e) => e,
        Err(_) => return None,
    };

    let event: Event = match serde_json::from_str(&text){
        Ok(e) => e,
        Err(_) => return None,
    };

    Some(event)
}



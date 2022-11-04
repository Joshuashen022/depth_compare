use std::collections::BTreeMap;
use std::fmt;
// use std::sync::{Arc, RwLock};
use serde::{de::Visitor, Deserialize, Deserializer, de::SeqAccess};
use ordered_float::OrderedFloat;
use anyhow::{Result, Error, anyhow};

#[derive(Deserialize, Debug)]
pub struct Event {
    #[serde(rename = "e")]
    pub ttype: String,
    #[serde(rename = "E")]
    pub ts: i64,
    #[serde(rename = "s")]
    pub pair: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    #[serde(rename = "b")]
    pub bids: Vec<DepthRow>,
    #[serde(rename = "a")]
    pub asks: Vec<DepthRow>,
}

impl Event {
    pub fn match_seq_num(&self, expected_id: &i64) -> bool {
        self.first_update_id == *expected_id
    }

    pub fn match_snapshot(&self, updated_id: i64) -> bool {
        let first = self.first_update_id <= updated_id + 1;
        let second = updated_id + 1 <= self.last_update_id;
        println!("{}, {}", first, second);
        self.first_update_id <= updated_id + 1 && updated_id + 1 <= self.last_update_id
    }
}

#[derive(Deserialize, Debug)]
pub struct LevelEvent {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,

    pub bids: Vec<DepthRow>,

    pub asks: Vec<DepthRow>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct DepthRow {
    pub price: f64,
    pub amount: f64,
}

impl<'de> Deserialize<'de> for DepthRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        deserializer.deserialize_tuple(2, DepthRowVisitor)
    }
}

struct DepthRowVisitor;

impl<'de> Visitor<'de> for DepthRowVisitor {
    type Value = DepthRow;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a map with keys 'first' and 'second'")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: SeqAccess<'de> {
        let mut price = None;
        let mut amount = None;

        if let Some(val) = seq.next_element::<&str>()? {
            match val.parse::<f64>() {
                Ok(num) => price = Some(num),
                Err(_) => return Err(serde::de::Error::custom("Fail to convert price str to f64")),
            }
        }

        if let Some(val) = seq.next_element::<&str>()? {
            match val.parse::<f64>() {
                Ok(num) => amount = Some(num),
                Err(_) => return Err(serde::de::Error::custom("Fail to convert amount str to f64")),
            }
        }

        if price.is_none() {
            return Err(serde::de::Error::custom("Missing price field"))
        }

        if amount.is_none() {
            return Err(serde::de::Error::custom("Missing amount field"))
        }

        Ok(DepthRow{price: price.unwrap(), amount: amount.unwrap()})
    }

}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSpotOrderBookSnapshot {
    pub last_update_id: i64,
    pub time_stamp: i64,
    pub bids: Vec<DepthRow>,
    pub asks: Vec<DepthRow>,
}
impl BinanceSpotOrderBookSnapshot{
    /// Compare self with other Order Book
    pub fn if_contains(&self, other: &BinanceSpotOrderBookSnapshot) -> bool {
        let mut contains_bids = true;
        let mut contains_asks = true;
        for bid in &other.bids{
            if !self.bids.contains(bid) {
                contains_bids = false;
                break
            }
        }

        for ask in &other.bids{
            if !self.asks.contains(&ask){
                contains_asks = false;
                break
            }
        }

        contains_bids && contains_asks
    }

    /// Find different `bids` and `asks`,
    /// and return as `(bids, asks)`
    pub fn find_different(&self, other: &BinanceSpotOrderBookSnapshot) -> (Vec<DepthRow>, Vec<DepthRow>) {
        let mut bid_different = Vec::new();
        let mut ask_different = Vec::new();

        for bid in &other.bids{
            if !self.bids.contains(bid) {
                bid_different.push(*bid);
                break
            }
        }

        for ask in &other.bids{
            if !self.asks.contains(&ask){
                ask_different.push(*ask);
                break
            }
        }

        (bid_different, ask_different)
    }
}

pub struct Shared {
    last_update_id: i64,
    time_stamp: i64,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
}

impl Shared {
    pub fn new() -> Self {
        Shared {
            last_update_id: 0,
            time_stamp: 0,
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
        }
    }

    /// return last_update_id
    pub fn id(&self) -> i64{
        self.last_update_id
    }

    pub fn load_snapshot(&mut self, snapshot: &BinanceSpotOrderBookSnapshot) {
        self.asks.clear();
        for ask in &snapshot.asks {
            self.asks.insert(OrderedFloat(ask.price), ask.amount);
        }

        self.bids.clear();
        for bid in &snapshot.bids {
            self.bids.insert(OrderedFloat(bid.price), bid.amount);
        }

        self.last_update_id = snapshot.last_update_id;
    }

    /// Only used for "Event"
    pub fn add_event(&mut self, event: Event) {
        for ask in event.asks {
            if ask.amount == 0.0 {
                self.asks.remove(&OrderedFloat(ask.price));
            } else {
                self.asks.insert(OrderedFloat(ask.price), ask.amount);
            }
        }

        for bid in event.bids {
            if bid.amount == 0.0 {
                self.bids.remove(&OrderedFloat(bid.price));
            } else {
                self.bids.insert(OrderedFloat(bid.price), bid.amount);
            }
        }

        self.last_update_id = event.last_update_id;
        self.time_stamp = event.ts;
    }

    /// Only used for "LevelEvent"
    pub fn set_level_event(&mut self, level_event: LevelEvent, time_stamp: i64){
        for ask in level_event.asks {
            if ask.amount == 0.0 {
                self.asks.remove(&OrderedFloat(ask.price));
            } else {
                self.asks.insert(OrderedFloat(ask.price), ask.amount);
            }
        }

        for bid in level_event.bids {
            if bid.amount == 0.0 {
                self.bids.remove(&OrderedFloat(bid.price));
            } else {
                self.bids.insert(OrderedFloat(bid.price), bid.amount);
            }
        }

        self.last_update_id = level_event.last_update_id;
        self.time_stamp = time_stamp;
    }

    pub fn get_snapshot(&self) -> BinanceSpotOrderBookSnapshot {
        let asks = self.asks
            .iter()
            .map(|(price, amount)| DepthRow {price: price.into_inner(), amount: *amount})
            .collect();

        let bids = self.bids
            .iter()
            .rev()
            .map(|(price, amount)| DepthRow {price: price.into_inner(), amount: *amount})
            .collect();
        let time_stamp = self.time_stamp;
        BinanceSpotOrderBookSnapshot {
            last_update_id: self.last_update_id,
            time_stamp,
            asks,
            bids,
        }
    }

    /// With give event to update snapshot,
    /// if event doesn't satisfy return error
    pub fn update_snapshot(&mut self, event: Event)-> Result<()>  {
        if event.first_update_id != self.last_update_id + 1 {
            Err(anyhow!(
                "Expect event u to be {}, found {}",
                self.last_update_id + 1,
                event.first_update_id
            ))
        } else{
            self.add_event(event);
            Ok(())
        }

    }
}

#[test]
fn depth_row(){
    let a = DepthRow{amount:1.0, price: 2.0};
    let b = DepthRow{amount:1.0, price: 2.0};
    assert_eq!(a, b);
}
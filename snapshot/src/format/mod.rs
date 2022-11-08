pub mod binance_spot;
pub mod binance_perpetual_c;
pub mod binance_perpetual_u;


use serde::{de::Visitor, Deserialize, Deserializer, de::SeqAccess};
use std::fmt;

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
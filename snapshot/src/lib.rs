
use std::borrow::Cow;

mod connection;
mod format;

use format::DepthRow;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

/// 行情类型: 现货、永续合约
pub enum OrderbookType {
    Spot,
    Perpetual,
}

/// 交易所类型
pub enum ExchangeType {
    Binance
}

/// 对应某个交易所里一个币对的行情订阅，对外提供接口支持获取最新截面数据
pub trait Orderbook {
    type SnapShotType: OrderbookSnapshot;
    fn get_snapshot(&self) -> Self::SnapShotType;
    fn get_type(&self) -> OrderbookType;
    fn get_exchange(&self) -> ExchangeType;
}

/// 被返回的截面数据需要支持的一些方法
pub trait OrderbookSnapshot {
    fn get_bids(&self) -> &Vec<DepthRow>;
    fn get_asks(&self) -> &Vec<DepthRow>;
    fn get_id(&self) -> Cow<str>;
    /// Time recorded in data or receive time
    /// (Linux time in `ms`)
    fn get_ts(&self) -> i64;
}
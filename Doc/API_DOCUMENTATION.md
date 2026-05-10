# A股实时监控中心 - API 接口文档

baseURL: `http://localhost:9527`

## 1. 市场数据接口

### 获取实时行情列表
- **URL**: `/api/market`
- **Method**: `GET`
- **Description**: 获取当前监控的所有指数和股票的实时行情数据。
- **Response**: `Array<StockInfo>`
  ```json
  [
    {
      "symbol": "sh000001",
      "name": "上证指数",
      "price": 3000.00,
      "change_percent": 0.5,
      "high": 3010.00,
      "low": 2990.00,
      "volume": 1000000.0,
      "open": 2995.00,
      "pre_close": 2985.00,
      "limit_up": 3283.50,
      "limit_down": 2686.50,
      "amount": 100000000.0,
      "turnover_rate": 0.0,
      "pe_ratio": 0.0
    }
  ]
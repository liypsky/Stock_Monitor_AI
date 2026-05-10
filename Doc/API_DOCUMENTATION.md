# A股实时监控中心 - API 接口文档

 baseURL: `http://localhost:9527`

## 1. 市场数据接口

### 获取实时行情列表
- **URL**: `/api/market`
- **Method**: `GET`
- **Description**: 获取当前监控的所有指数和股票的实时行情数据。
- **Response**: `Array<StockInfo>`
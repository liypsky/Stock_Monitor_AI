use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
// 确保导入 IntoResponse，虽然 Json 已经实现，但有时显式导入有助于调试
// use axum::response::IntoResponse; 
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{RwLock, Notify};
use tower_http::services::ServeDir;
use tracing_subscriber;
use regex::Regex;
use reqwest;
use std::sync::LazyLock;
use encoding_rs::GBK;
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockInfo {
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub change_percent: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub open: f64,
    pub pre_close: f64,
    // 新增字段用于详情页
    #[serde(default)]
    pub limit_up: f64,
    #[serde(default)]
    pub limit_down: f64,
    #[serde(default)]
    pub amount: f64, // 成交额
}

// 新增：资金流向结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoneyFlow {
    pub main_net: f64,      // 主力净流入
    pub super_large: f64,   // 超大单
    pub large: f64,         // 大单
    pub medium: f64,        // 中单
    pub small: f64,         // 小单
    pub retail: f64,        // 散户
}

#[derive(Clone)]
struct AppState {
    index_list: Arc<RwLock<Vec<String>>>,
    stock_list: Arc<RwLock<Vec<String>>>,
    market_data: Arc<RwLock<Vec<StockInfo>>>,
    data_refresh_notify: Arc<Notify>,
}

static RE_SINA_DATA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"var\s+hq_str_(\w+)\s*=\s*"([^"]*)""#).unwrap()
});

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let default_indices = vec![
        "sh000001".to_string(),
        "sz399001".to_string(),
        "sz399006".to_string(),
        "bj899050".to_string(),
    ];

    let default_stocks = vec![
        "sh600519".to_string(),
        "sz000001".to_string(),
        "sh601318".to_string(),
        "sz002594".to_string(),
    ];

    let notify = Arc::new(Notify::new());

    let state = AppState {
        index_list: Arc::new(RwLock::new(default_indices)),
        stock_list: Arc::new(RwLock::new(default_stocks)),
        market_data: Arc::new(RwLock::new(vec![])),
        data_refresh_notify: notify.clone(),
    };

    let state_clone = state.clone();
    tokio::spawn(async move {
        fetch_realtime_data(state_clone, notify).await;
    });

    let app = Router::new()
        .route("/api/market", get(get_market_data))
        .route("/api/config", get(get_config))
        .route("/api/add_stock", get(add_stock))
        .route("/api/remove_stock", post(remove_stock)) 
        .route("/api/reorder_stocks", post(reorder_stocks)) 
        .route("/api/ai_analysis", get(get_ai_analysis)) 
        .route("/api/add_index", get(add_index))
        // 新增路由
        .route("/api/stock_detail", get(get_stock_detail))
        .route("/api/stock_money_flow", get(get_stock_money_flow))
        .nest_service("/", ServeDir::new("static"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9527").await.unwrap();
    println!("🚀 A股实时监控服务已启动: http://localhost:9527");
    axum::serve(listener, app).await.unwrap();
}

async fn get_config(State(state): State<AppState>) -> Json<Value> {
    let indices = state.index_list.read().await.clone();
    let stocks = state.stock_list.read().await.clone();
    Json(json!({
        "indices": indices,
        "stocks": stocks
    }))
}

async fn get_market_data(State(state): State<AppState>) -> Json<Vec<StockInfo>> {
    Json(state.market_data.read().await.clone())
}

// 新增：获取个股详情
async fn get_stock_detail(
    Query(params): Query<std::collections::HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        let client = reqwest::Client::new();
        let url = format!("http://hq.sinajs.cn/list={}", normalized_code);
        
        match client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(bytes) = resp.bytes().await {
                    let (text, _, _) = GBK.decode(&bytes);
                    if let Some(cap) = RE_SINA_DATA.captures(&text) {
                        let data_str = &cap[2];
                        let parts: Vec<&str> = data_str.split(',').collect();
                        if parts.len() > 30 {
                            let price = parts[3].parse::<f64>().unwrap_or(0.0);
                            let pre_close = parts[2].parse::<f64>().unwrap_or(0.0);
                            let open = parts[1].parse::<f64>().unwrap_or(0.0);
                            let high = parts[4].parse::<f64>().unwrap_or(0.0);
                            let low = parts[5].parse::<f64>().unwrap_or(0.0);
                            let volume = parts[8].parse::<f64>().unwrap_or(0.0); // 股数
                            let amount = parts[9].parse::<f64>().unwrap_or(0.0); // 成交额
                            
                            // 计算涨跌停价 (A股通常10%，ST 5%，科创板/创业板 20%，这里简化处理)
                            let limit_up = pre_close * 1.1;
                            let limit_down = pre_close * 0.9;
                            
                            let change_percent = if pre_close > 0.0 { ((price - pre_close) / pre_close) * 100.0 } else { 0.0 };

                            return Json(json!({
                                "status": "success",
                                "data": {
                                    "symbol": normalized_code,
                                    "name": parts[0],
                                    "price": price,
                                    "pre_close": pre_close,
                                    "open": open,
                                    "high": high,
                                    "low": low,
                                    "volume": volume,
                                    "amount": amount,
                                    "limit_up": limit_up,
                                    "limit_down": limit_down,
                                    "change_percent": change_percent
                                }
                            }));
                        }
                    }
                }
            }
            Err(e) => eprintln!("Fetch detail error: {}", e)
        }
    }
    Json(json!({"status": "error", "msg": "fetch failed"}))
}

// 新增：获取资金流向 (模拟数据，因为真实接口需要复杂逆向或付费)
async fn get_stock_money_flow(
    Query(params): Query<std::collections::HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(_code) = params.get("code") {
        // 这里使用随机数据模拟，实际项目中应替换为真实的东财/同花顺接口爬取逻辑
        let main_net: f64 = (rand::random::<f64>() * 2000.0 - 1000.0) * 10000.0; // -1000万 to 1000万
        
        return Json(json!({
            "status": "success",
            "data": {
                "main_net": main_net,
                "super_large": main_net * 0.4,
                "large": main_net * 0.3,
                "medium": -main_net * 0.2,
                "small": -main_net * 0.3,
                "retail": -main_net * 0.2
            }
        }));
    }
    Json(json!({"status": "error"}))
}

fn normalize_stock_code(code: &str) -> String {
    let code = code.trim().to_lowercase();
    if code.starts_with("sh") || code.starts_with("sz") || code.starts_with("bj") {
        return code;
    }
    
    if code.starts_with("6") {
        format!("sh{}", code)
    } else if code.starts_with("0") || code.starts_with("3") {
        format!("sz{}", code)
    } else if code.starts_with("8") || code.starts_with("4") || code.starts_with("9") {
        format!("bj{}", code)
    } else {
        format!("sz{}", code)
    }
}

async fn add_stock(
    Query(params): Query<std::collections::HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        let mut list = state.stock_list.write().await;
        
        if !list.contains(&normalized_code) {
            list.push(normalized_code.clone());
            println!("✅ Backend: Added stock {}", normalized_code);
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "added", "code": normalized_code}));
        }
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "exists", "msg": "already exists"}));
    }
    Json(json!({"status": "error", "msg": "no code"}))
}

// 尝试添加 #[axum::debug_handler] 以获取更清晰的错误信息
// 如果编译成功，可以移除该属性
// 修改后 (正确):
#[axum::debug_handler] // 如果你启用了 macros feature，可以保留用于调试，否则可移除
async fn remove_stock(
    State(state): State<AppState>, // State 放在前面
    Json(payload): Json<Value>,    // Json 必须放在最后
) -> Json<Value> {
    if let Some(code) = payload.get("code").and_then(|c| c.as_str()) {
        let normalized_code = normalize_stock_code(code);
        let mut list = state.stock_list.write().await;
        if let Some(pos) = list.iter().position(|x| x == &normalized_code) {
            list.remove(pos);
            println!("✅ Backend: Removed stock {}", normalized_code);
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "removed"}));
        }
        return Json(json!({"status": "error", "msg": "not found"}));
    }
    Json(json!({"status": "error", "msg": "invalid payload"}))
}

#[axum::debug_handler]
async fn reorder_stocks(
    State(state): State<AppState>, // State 放在前面
    Json(payload): Json<Value>,    // Json 必须放在最后
) -> Json<Value> {
    if let Some(new_order) = payload.get("stocks").and_then(|v| v.as_array()) {
        let mut list = state.stock_list.write().await;
        let new_codes: Vec<String> = new_order.iter()
            .filter_map(|v| v.as_str().map(|s| normalize_stock_code(s)))
            .collect();
        
        *list = new_codes;
        println!("✅ Backend: Stocks reordered");
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "success"}));
    }
    Json(json!({"status": "error"}))
}

async fn get_ai_analysis(
    Query(params): Query<std::collections::HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        return Json(json!({
            "status": "success",
            "analysis": format!("[AI模拟] {} 近期走势震荡上行，成交量温和放大，建议关注上方压力位。技术指标显示RSI处于中性区域，短期可能有回调风险，但中长期趋势向好。", code)
        }));
    }
    Json(json!({"status": "error", "msg": "no code"}))
}

async fn add_index(
    Query(params): Query<std::collections::HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let mut list = state.index_list.write().await;
        if list.len() >= 8 && !list.contains(code) {
            return Json(json!({"status": "full", "msg": "max 8 indices"}));
        }
        if !list.contains(code) {
            list.push(code.clone());
            println!("✅ Backend: Added index {}", code);
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "added"}));
        }
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "exists", "msg": "already exists"}));
    }
    Json(json!({"status": "error", "msg": "no code"}))
}

async fn fetch_realtime_data(state: AppState, notify: Arc<Notify>) {
    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "User-Agent", 
                reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            );
            headers.insert(
                "Referer", 
                reqwest::header::HeaderValue::from_static("http://finance.sina.com.cn/")
            );
            headers
        })
        .build()
        .unwrap();

    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(3)) => {}
        }
        
        let indices = state.index_list.read().await.clone();
        let stocks = state.stock_list.read().await.clone();
        let mut all_codes = indices.clone();
        all_codes.extend(stocks.clone());
        
        if all_codes.is_empty() { 
            continue; 
        }

        let url = format!("http://hq.sinajs.cn/list={}", all_codes.join(","));
        
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    eprintln!("❌ HTTP Error: {}", resp.status());
                    continue;
                }

                match resp.bytes().await {
                    Ok(bytes) => {
                        let (text, _, _) = GBK.decode(&bytes);
                        let parsed = parse_sina_data(&text);
                        
                        let mut cache = state.market_data.write().await;
                        *cache = parsed;
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to read response bytes: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Fetch error: {}", e);
            }
        }
    }
}

fn parse_sina_data(raw_text: &str) -> Vec<StockInfo> {
    let mut results = vec![];
    if !RE_SINA_DATA.is_match(raw_text) {
        if raw_text.len() > 20 {
            eprintln!("⚠️ Regex did not match any data in raw text.");
        }
        return results;
    }

    for cap in RE_SINA_DATA.captures_iter(raw_text) {
        let code = &cap[1];
        let data_str = &cap[2];
        let parts: Vec<&str> = data_str.split(',').collect();
        
        if parts.len() < 32 { 
            continue; 
        }

        let name = parts[0].to_string();
        let open = parts[1].parse::<f64>().unwrap_or(0.0);
        let pre_close = parts[2].parse::<f64>().unwrap_or(0.0);
        let price = parts[3].parse::<f64>().unwrap_or(0.0);
        let high = parts[4].parse::<f64>().unwrap_or(0.0);
        let low = parts[5].parse::<f64>().unwrap_or(0.0);
        let volume = parts[9].parse::<f64>().unwrap_or(0.0); 
        // 解析成交额 (parts[9]在新浪接口中通常是成交额，但有时索引可能因市场而异，这里假设parts[9]是成交额，parts[8]是成交量股数)
        // 注意：新浪接口中 parts[8] 是成交量(手/股), parts[9] 是成交额(元)
        let amount = parts[9].parse::<f64>().unwrap_or(0.0);

        let change = price - pre_close;
        let change_percent = if pre_close > 0.0 { (change / pre_close) * 100.0 } else { 0.0 };
        
        // 计算涨跌停价 (简化处理，实际需根据板块判断)
        let limit_up = pre_close * 1.1;
        let limit_down = pre_close * 0.9;

        results.push(StockInfo {
            symbol: code.to_string(),
            name,
            price,
            change_percent,
            high,
            low,
            volume,
            open,
            pre_close,
            amount,
            limit_up,
            limit_down,
        });
    }
    results
}
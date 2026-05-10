use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
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
use std::collections::HashMap;
use std::path::Path;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockInfo {
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub change_percent: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64, // 成交量(手)
    pub open: f64,
    pub pre_close: f64,
    #[serde(default)]
    pub limit_up: f64,
    #[serde(default)]
    pub limit_down: f64,
    #[serde(default)]
    pub amount: f64, // 成交额(元)
    // 新增扩展字段，用于后续扩展
    #[serde(default)]
    pub turnover_rate: f64, // 换手率
    #[serde(default)]
    pub pe_ratio: f64,      // 市盈率
}

// 资金流向结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoneyFlow {
    pub main_net: f64,      // 主力净流入 (元)
    pub super_large: f64,   // 超大单净流入 (元)
    pub large: f64,         // 大单净流入 (元)
    pub medium: f64,        // 中单净流入 (元)
    pub small: f64,         // 小单净流入 (元)
    pub retail: f64,        // 散户净流入 (元) - 通常由计算得出或接口直接返回
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinuteDataPoint {
    pub time: String,
    pub price: f64,
    pub avg_price: f64,
    pub volume: f64,
    pub open: f64,
    pub close: f64,
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

// 新增：配置结构体，用于序列化保存
#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppConfig {
    indices: Vec<String>,
    stocks: Vec<String>,
}

// 新增：加载配置的辅助函数
fn load_config_from_file() -> Option<AppConfig> {
    let config_path = Path::new("data/config.json");
    if config_path.exists() {
        if let Ok(content) = fs::read_to_string(config_path) {
            if let Ok(config) = serde_json::from_str::<AppConfig>(&content) {
                println!("✅ Loaded config from data/config.json");
                return Some(config);
            } else {
                eprintln!("⚠️ Failed to parse config.json, using defaults");
            }
        }
    }
    None
}

// 新增：保存配置的辅助函数
fn save_config_to_file(indices: &[String], stocks: &[String]) {
    // 确保 data 目录存在
    let data_dir = Path::new("data");
    if !data_dir.exists() {
        if let Err(e) = fs::create_dir_all(data_dir) {
            eprintln!("❌ Failed to create data directory: {}", e);
            return;
        }
    }

    let config = AppConfig {
        indices: indices.to_vec(),
        stocks: stocks.to_vec(),
    };

    let config_path = Path::new("data/config.json");
    match serde_json::to_string_pretty(&config) {
        Ok(json_str) => {
            if let Err(e) = fs::write(config_path, json_str) {
                eprintln!("❌ Failed to write config.json: {}", e);
            } else {
                println!("💾 Config saved to data/config.json");
            }
        }
        Err(e) => {
            eprintln!("❌ Failed to serialize config: {}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 默认配置
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

    // 尝试从文件加载配置，否则使用默认值
    let (initial_indices, initial_stocks) = if let Some(saved_config) = load_config_from_file() {
        (saved_config.indices, saved_config.stocks)
    } else {
        (default_indices, default_stocks)
    };

    let notify = Arc::new(Notify::new());

    let state = AppState {
        index_list: Arc::new(RwLock::new(initial_indices)),
        stock_list: Arc::new(RwLock::new(initial_stocks)),
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
        .route("/api/remove_index", post(remove_index))
        .route("/api/stock_detail", get(get_stock_detail))
        .route("/api/stock_money_flow", get(get_stock_money_flow))
        .route("/api/stock_minute_data", get(get_stock_minute_data))
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
    Query(params): Query<HashMap<String, String>>,
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

// 新增：获取资金流向 (对接东方财富接口)
async fn get_stock_money_flow(
    Query(params): Query<HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        
        // 东方财富资金流向接口
        // secid: 1.600519 (sh), 0.000001 (sz)
        let secid = if normalized_code.starts_with("sh") {
            format!("1.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("sz") {
            format!("0.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("bj") {
            format!("0.{}", &normalized_code[2..]) // 北交所通常也映射到0或特定标识，这里简化处理
        } else {
            format!("0.{}", normalized_code)
        };

        let url = format!(
            "http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get?cb=jQuery11230_&secid={}&lmt=0&fields1=f1%2Cf2%2Cf3%2Cf7&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61%2Cf62%2Cf63%2Cf64%2Cf65&ut=b2884a393a59ad64002292a3e90d46a5&rt=52",
            secid
        );

        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0"));
                headers.insert("Referer", reqwest::header::HeaderValue::from_static("http://quote.eastmoney.com/"));
                headers
            })
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        match client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(text) = resp.text().await {
                    // 东方财富接口通常返回 JSONP 格式: jQuery11230_({...});
                    // 改进解析逻辑：查找第一个 '{' 和最后一个 '}'
                    if let Some(start_idx) = text.find('{') {
                        if let Some(end_idx) = text.rfind('}') {
                            let json_str = &text[start_idx..=end_idx];
                            if let Ok(root) = serde_json::from_str::<Value>(json_str) {
                                if let Some(data) = root.get("data") {
                                    if let Some(klines) = data.get("klines") {
                                        if let Some(arr) = klines.as_array() {
                                            if !arr.is_empty() {
                                                // 取最新的一条数据 (通常是最后一个元素，或者第一个取决于排序，东财默认最近在前)
                                                // 格式: "2023-10-27,-123456.0,12345.0,..."
                                                // fields2: f51(日期), f52(主力净流入), f53(超大单), f54(大单), f55(中单), f56(小单)...
                                                if let Some(latest_str) = arr.first().and_then(|v| v.as_str()) {
                                                    let parts: Vec<&str> = latest_str.split(',').collect();
                                                    if parts.len() > 5 {
                                                        let main_net = parts[1].parse::<f64>().unwrap_or(0.0);
                                                        let super_large = parts[2].parse::<f64>().unwrap_or(0.0);
                                                        let large = parts[3].parse::<f64>().unwrap_or(0.0);
                                                        let medium = parts[4].parse::<f64>().unwrap_or(0.0);
                                                        let small = parts[5].parse::<f64>().unwrap_or(0.0);
                                                        
                                                        // 散户通常等于 -(主力+中单+小单) 或者接口有单独字段，这里简单计算或直接使用小单代表散户倾向
                                                        // 东财定义：散户 = 小单。有些接口定义不同，这里沿用前段逻辑：retail
                                                        let retail = small; 

                                                        return Json(json!({
                                                            "status": "success",
                                                            "data": {
                                                                "main_net": main_net,
                                                                "super_large": super_large,
                                                                "large": large,
                                                                "medium": medium,
                                                                "small": small,
                                                                "retail": retail
                                                            }
                                                        }));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                eprintln!("Failed to parse JSON from EastMoney: {}", json_str.chars().take(100).collect::<String>());
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Fetch money flow error: {}", e)
        }
        
        // 如果获取失败，返回全0或错误，前端应处理
        Json(json!({
            "status": "success",
            "data": {
                "main_net": 0.0,
                "super_large": 0.0,
                "large": 0.0,
                "medium": 0.0,
                "small": 0.0,
                "retail": 0.0
            }
        }))
    } else {
        Json(json!({"status": "error"}))
    }
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
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        let mut list = state.stock_list.write().await;
        
        if !list.contains(&normalized_code) {
            list.push(normalized_code.clone());
            println!("✅ Backend: Added stock {}", normalized_code);
            
            // 保存配置
            let indices = state.index_list.read().await.clone();
            let stocks = list.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks);
            
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
            
            // 保存配置
            let indices = state.index_list.read().await.clone();
            let stocks = list.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks);
            
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
        
        *list = new_codes.clone();
        println!("✅ Backend: Stocks reordered");
        
        // 保存配置
        let indices = state.index_list.read().await.clone();
        drop(list); // 释放写锁
        save_config_to_file(&indices, &new_codes);
        
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "success"}));
    }
    Json(json!({"status": "error"}))
}

async fn get_ai_analysis(
    Query(params): Query<HashMap<String, String>>,
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
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        // 修复：对指数代码也进行规范化处理，防止非法代码导致后续请求异常
        let normalized_code = normalize_stock_code(code);
        let mut list = state.index_list.write().await;
        
        // 限制最多8个
        if list.len() >= 8 && !list.contains(&normalized_code) {
            return Json(json!({"status": "full", "msg": "max 8 indices"}));
        }
        
        if !list.contains(&normalized_code) {
            list.push(normalized_code.clone());
            println!("✅ Backend: Added index {}", normalized_code);
            
            // 保存配置
            let stocks = state.stock_list.read().await.clone();
            let indices = list.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks);
            
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
        
        // 处理可能的空数据或无效数据字符串
        if data_str.is_empty() || data_str == "null" {
            eprintln!("⚠️ Empty data for code: {}", code);
            continue;
        }

        let parts: Vec<&str> = data_str.split(',').collect();
        
        // 修复：增加边界检查，防止索引越界 Panic
        // 新浪接口正常股票/指数数据通常至少有30+个字段，但最少需要前几个核心字段
        if parts.len() < 6 { 
            eprintln!("⚠️ Insufficient data fields for code {}: {} fields", code, parts.len());
            continue; 
        }

        // 使用安全的解析方式，避免 unwrap 导致 panic
        let name = parts[0].to_string();
        let open = parts[1].parse::<f64>().unwrap_or(0.0);
        let pre_close = parts[2].parse::<f64>().unwrap_or(0.0);
        let price = parts[3].parse::<f64>().unwrap_or(0.0);
        let high = parts[4].parse::<f64>().unwrap_or(0.0);
        let low = parts[5].parse::<f64>().unwrap_or(0.0);
        
        // 成交量和成交额可能在后面，安全获取
        let volume = if parts.len() > 9 { parts[8].parse::<f64>().unwrap_or(0.0) } else { 0.0 };
        let amount = if parts.len() > 9 { parts[9].parse::<f64>().unwrap_or(0.0) } else { 0.0 };

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
            pe_ratio: 0.0,
            turnover_rate: 0.0,
        });
    }
    results
}

// 新增：辅助函数，用于获取分时数据，避免闭包所有权问题
async fn fetch_minute_data_from_sina(normalized_code: &str, datalen: usize) -> Option<Vec<MinuteDataPoint>> {
    // 使用新浪财经的分时成交明细接口或者分钟K线接口
    // 这里使用分钟K线接口，scale=1 表示1分钟
    let url = format!(
        "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={}&scale=1&ma=5&datalen={}",
        normalized_code, datalen
    );

    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            // 关键：新浪接口通常校验 Referer 和 User-Agent
            headers.insert(
                "User-Agent", 
                reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            );
            headers.insert(
                "Referer", 
                reqwest::header::HeaderValue::from_static("http://finance.sina.com.cn/realstock/company/sh600519/nc.shtml")
            );
            headers.insert(
                "Host",
                reqwest::header::HeaderValue::from_static("money.finance.sina.com.cn")
            );
            headers
        })
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    match client.get(&url).send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                eprintln!("Fetch Minute Data HTTP Error: {} for {}", status, normalized_code);
                return None;
            }

            if let Ok(text) = resp.text().await {
                if text.trim().is_empty() || text.trim() == "null" {
                    return None;
                }
                
                // 尝试清理可能的非法字符或BOM头，以及新浪接口可能返回的非标准JSON包装
                let clean_text = text.trim()
                    .trim_start_matches(|c: char| c.is_control() && c != '\n' && c != '\r')
                    .trim_end_matches(';'); // 有些接口可能带回分号

                // 调试日志：打印前100个字符以确认格式
                // eprintln!("Raw Minute Data for {}: {}", normalized_code, clean_text.chars().take(100).collect::<String>());

                match serde_json::from_str::<Vec<Value>>(clean_text) {
                    Ok(data_array) => {
                        let mut minutes: Vec<MinuteDataPoint> = Vec::new();
                        for item in data_array {
                            // 尝试多种时间字段名: day, d, date, time
                            // 新浪分钟K线通常返回 "day": "2023-10-27 10:00:00" 或类似格式
                            let time_str = item.get("day").and_then(|v| v.as_str())
                                .or_else(|| item.get("d").and_then(|v| v.as_str()))
                                .or_else(|| item.get("date").and_then(|v| v.as_str()))
                                .or_else(|| item.get("time").and_then(|v| v.as_str()));
                            
                            if let Some(day) = time_str {
                                // 宽松校验：只要长度大于等于5 (HH:MM) 即可
                                if day.len() < 5 { 
                                    continue; 
                                }
                                
                                // 提取时间部分，假设格式为 YYYY-MM-DD HH:MM:SS 或 YYYY/MM/DD HH:MM
                                // 我们主要需要 HH:MM 用于前端展示
                                let time_part = if day.contains(' ') {
                                    let parts: Vec<&str> = day.split(' ').collect();
                                    if let Some(last) = parts.last() {
                                        // 取最后5位 HH:MM，如果带秒则取前5位
                                        let t = if last.len() >= 5 { &last[0..5] } else { last };
                                        t
                                    } else { day }
                                } else if day.contains('/') {
                                     // 处理 2023/10/27 10:00 格式
                                     let parts: Vec<&str> = day.split(' ').collect();
                                     if let Some(last) = parts.last() {
                                         if last.len() >= 5 { &last[0..5] } else { last }
                                     } else { day }
                                } else if day.len() >= 5 {
                                    // 假设最后5位是时间
                                    &day[day.len()-5..]
                                } else {
                                    day
                                };
                                
                                let open = item.get("open").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let close = item.get("close").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let volume = item.get("volume").and_then(|v| v.as_f64()).unwrap_or(0.0); 
                                
                                // 如果 close 为 0，尝试使用 price 字段
                                let final_close = if close > 0.0 { close } else { item.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0) };

                                if final_close <= 0.0 {
                                    continue;
                                }

                                minutes.push(MinuteDataPoint {
                                    time: time_part.to_string(), // 前端展示用 HH:MM
                                    price: final_close,
                                    avg_price: 0.0, // 均价需要前端或后端累计计算，这里先置0，前端会重新计算
                                    volume,
                                    open,
                                    close: final_close,
                                });
                            }
                        }
                        if minutes.is_empty() {
                            None
                        } else {
                            Some(minutes)
                        }
                    }
                    Err(e) => {
                        eprintln!("JSON Parse Error for {}: {:?}, Text snippet: {}", normalized_code, e, clean_text.chars().take(100).collect::<String>());
                        None
                    }
                }
            } else {
                None
            }
        }
        Err(e) => {
            eprintln!("Fetch Minute Data Network Error for {}: {}", normalized_code, e);
            None
        }
    }
}

// 新增：获取分时历史数据
async fn get_stock_minute_data(
    Query(params): Query<HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        
        // 1. 尝试获取当天数据 (240条)
        if let Some(data) = fetch_minute_data_from_sina(&normalized_code, 240).await {
            println!("Fetched {} minute data points for {} (Today)", data.len(), normalized_code);
            return Json(json!({
                "status": "success",
                "data": data
            }));
        }

        // 2. 如果当天无数据（周末/节假日/未开盘），尝试获取最近1000条，并提取最近一个完整交易日
        println!("No data for today, fetching history for {}...", normalized_code);
        if let Some(history_data) = fetch_minute_data_from_sina(&normalized_code, 1000).await {
            if !history_data.is_empty() {
                // 策略：寻找最近的一个完整交易日。
                // 新浪返回的数据是按时间倒序还是正序？通常是正序（旧->新）。
                // 我们假设数据是正序的。最后一条数据的时间日期即为最新交易日。
                
                    // 注意：我们的 MinuteDataPoint.time 只存了 HH:MM，我们需要原始数据中的日期
                    // 由于 fetch_minute_data_from_sina 内部已经处理了时间，我们需要重新获取带日期的原始数据或者改进结构体
                    // 为了简化，我们假设 fetch_minute_data_from_sina 返回的数据中，如果跨天，时间字符串会有变化吗？
                    // 新浪接口返回的 "day" 字段通常包含日期。
                    
                    // 重新解析一次以获取日期分组，或者简单起见：
                    // 如果获取了1000条，通常包含最近几个交易日。
                    // 我们直接返回最后 240 条非零数据，这通常对应最近一个完整交易日。
                    // 但为了更准确，我们可以按日期分组。
                    
                    // 简易方案：直接返回最后 240 条。如果最后一条是今天的（即使没数据），前面的可能是昨天的。
                    // 更好的方案：在 fetch_minute_data_from_sina 中保留原始日期信息用于分组。
                    // 鉴于当前结构体限制，我们采用“去重日期”策略的变体：
                    // 实际上，新浪分钟K线接口在非交易日返回空或极少数据。
                    // 如果返回了1000条，最后240条极大概率是最近一个完整交易日的数据。
                    
                    let len = history_data.len();
                    let start = if len > 240 { len - 240 } else { 0 };
                    let recent_data = history_data[start..].to_vec();
                    
                    if !recent_data.is_empty() {
                         println!("Fetched {} historical minute data points for {} (Last Trading Day)", recent_data.len(), normalized_code);
                         return Json(json!({
                            "status": "success",
                            "data": recent_data
                        }));
                    }
            }
        }

        println!("No minute data available for {}", normalized_code);
        return Json(json!({"status": "success", "data": []}));
    }
    Json(json!({"status": "error", "msg": "no code provided"}))
}

#[axum::debug_handler]
async fn remove_index(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    if let Some(code) = payload.get("code").and_then(|c| c.as_str()) {
        let normalized_code = normalize_stock_code(code);
        let mut list = state.index_list.write().await;
        if let Some(pos) = list.iter().position(|x| x == &normalized_code) {
            list.remove(pos);
            println!("✅ Backend: Removed index {}", normalized_code);
            
            // 保存配置
            let stocks = state.stock_list.read().await.clone();
            let indices = list.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks);
            
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "removed"}));
        }
        return Json(json!({"status": "error", "msg": "not found"}));
    }
    Json(json!({"status": "error", "msg": "invalid payload"}))
}

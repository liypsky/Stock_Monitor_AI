use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::Notify;
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
use chrono::Datelike;

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

// 新增：K线数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KLineDataPoint {
    pub date: String,   // YYYY-MM-DD
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,    // 手
    pub amount: f64,    // 元
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub config_type: String,
    #[serde(rename = "apiUrl")]
    pub api_url: String,
    #[serde(rename = "apiKey", default)]
    pub api_key: String,
    #[serde(rename = "mainModels", default)]
    pub main_models: String,
    #[serde(rename = "fallbackModels", default)]
    pub fallback_models: String,
    pub model: String,
    #[serde(rename = "timeoutSeconds", default)]
    pub timeout_seconds: u64,
}

#[derive(Clone)]
struct AppState {
    index_list: Arc<RwLock<Vec<String>>>,
    stock_list: Arc<RwLock<Vec<String>>>,
    market_data: Arc<RwLock<Vec<StockInfo>>>,
    data_refresh_notify: Arc<Notify>,
    refresh_interval: Arc<RwLock<u64>>, // 新增：刷新间隔
    ai_configs: Arc<RwLock<Vec<AiConfig>>>, // 新增：AI配置
}

static RE_SINA_DATA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"var\s+hq_str_(\w+)\s*=\s*"([^"]*)""#).unwrap()
});

// 新增：配置结构体，用于序列化保存
#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppConfig {
    indices: Vec<String>,
    stocks: Vec<String>,
    #[serde(default = "default_data_fetch_interval")]
    data_fetch_interval: u64, // 后端获取数据的间隔
    #[serde(default = "default_page_refresh_interval")]
    page_refresh_interval: u64, // 前端页面刷新的间隔
    #[serde(default = "default_ai_configs")]
    ai_configs: Vec<AiConfig>,
}

fn default_data_fetch_interval() -> u64 {
    10
}

fn default_page_refresh_interval() -> u64 {
    3
}

fn default_ai_configs() -> Vec<AiConfig> {
    vec![]
}

// 新增：加载配置的辅助函数
fn load_config_from_file() -> Option<AppConfig> {
    // 修改：配置文件路径改为 setting/config.json
    let config_path = Path::new("setting/config.json");
    if config_path.exists() {
        if let Ok(content) = fs::read_to_string(config_path) {
            if let Ok(mut config) = serde_json::from_str::<AppConfig>(&content) {
                // 兼容旧配置
                if config.data_fetch_interval == 0 {
                    config.data_fetch_interval = 10;
                }
                if config.page_refresh_interval == 0 {
                    config.page_refresh_interval = 3;
                }
                println!("✅ Loaded config from setting/config.json");
                return Some(config);
            } else {
                eprintln!("⚠️ Failed to parse setting/config.json, using defaults");
            }
        }
    }
    None
}

// 新增：保存配置的辅助函数
fn save_config_to_file(indices: &[String], stocks: &[String], data_fetch_interval: u64, page_refresh_interval: u64, ai_configs: &[AiConfig]) {
    // 修改：确保 setting 目录存在
    let data_dir = Path::new("setting");
    if !data_dir.exists() {
        if let Err(e) = fs::create_dir_all(data_dir) {
            eprintln!("❌ Failed to create setting directory: {}", e);
            return;
        }
    }

    let config = AppConfig {
        indices: indices.to_vec(),
        stocks: stocks.to_vec(),
        data_fetch_interval,
        page_refresh_interval,
        ai_configs: ai_configs.to_vec(),
    };

    // 修改：配置文件路径改为 setting/config.json
    let config_path = Path::new("setting/config.json");
    match serde_json::to_string_pretty(&config) {
        Ok(json_str) => {
            if let Err(e) = fs::write(config_path, json_str) {
                eprintln!("❌ Failed to write setting/config.json: {}", e);
            } else {
                println!("💾 Config saved to setting/config.json");
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

    let default_data_interval = 10;
    let default_page_interval = 3;
    
    // 默认 AI 配置
    let default_ai_configs = vec![
        AiConfig {
            id: "default_1".to_string(),
            name: "默认配置".to_string(),
            config_type: "openai".to_string(),
            api_url: "".to_string(),
            api_key: "".to_string(),
            main_models: "gpt-3.5-turbo".to_string(),
            fallback_models: "".to_string(),
            model: "gpt-3.5-turbo".to_string(),
            timeout_seconds: 60,
        }
    ];

    // 尝试从文件加载配置，否则使用默认值
    let (initial_indices, initial_stocks, initial_data_interval, _initial_page_interval, initial_ai_configs) = if let Some(saved_config) = load_config_from_file() {
        (saved_config.indices, saved_config.stocks, saved_config.data_fetch_interval, saved_config.page_refresh_interval, saved_config.ai_configs)
    } else {
        println!("⚠️ Using default configuration as no valid config file found.");
        (default_indices, default_stocks, default_data_interval, default_page_interval, default_ai_configs)
    };

    println!("📋 Loaded Indices: {:?}", initial_indices);
    println!("📋 Loaded Stocks: {:?}", initial_stocks);

    let notify = Arc::new(Notify::new());

    let state = AppState {
        index_list: Arc::new(RwLock::new(initial_indices)),
        stock_list: Arc::new(RwLock::new(initial_stocks)),
        market_data: Arc::new(RwLock::new(vec![])),
        data_refresh_notify: notify.clone(),
        refresh_interval: Arc::new(RwLock::new(initial_data_interval)), // 初始化刷新间隔
        ai_configs: Arc::new(RwLock::new(initial_ai_configs)), // 初始化 AI 配置
    };

    let state_clone = state.clone();
    tokio::spawn(async move {
        fetch_realtime_data(state_clone, notify).await;
    });

    let app = Router::new()
        .route("/api/market", get(get_market_data))
        .route("/api/config", get(get_config))
        .route("/api/update_config", post(update_config))
        .route("/api/add_stock", get(add_stock))
        .route("/api/remove_stock", post(remove_stock)) 
        .route("/api/reorder_stocks", post(reorder_stocks)) 
        .route("/api/ai_analysis", get(get_ai_analysis)) 
        .route("/api/add_index", get(add_index))
        .route("/api/remove_index", post(remove_index))
        .route("/api/stock_detail", get(get_stock_detail))
        .route("/api/stock_money_flow", get(get_stock_money_flow))
        .route("/api/stock_minute_data", get(get_stock_minute_data))
        .route("/api/stock_kline_data", get(get_stock_kline_data)) // 新增路由
        .route("/api/ai_config", get(get_ai_config)) // 新增：获取AI配置
        .route("/api/update_ai_config", post(update_ai_config)) // 新增：更新AI配置
        .nest_service("/", ServeDir::new("static"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9527").await.unwrap();
    println!("🚀 A股实时监控服务已启动: http://localhost:9527");
    axum::serve(listener, app).await.unwrap();
}

async fn get_config(State(state): State<AppState>) -> Json<Value> {
    let indices = state.index_list.read().await.clone();
    let stocks = state.stock_list.read().await.clone();
    let data_interval = state.refresh_interval.read().await.clone();
    
    // 从文件读取最新的 page_refresh_interval，或者使用默认值
    // 为了简单，这里假设 page_refresh_interval 不常变，或者我们可以扩展 AppState 来存储它
    // 鉴于 AppState 修改较大，这里我们保存在文件中。前端加载配置后自行设置定时器。
    
    // 由于 AppState 定义已固定，为了不大幅重构，我们在这里做一个妥协：
    // 重新读取配置文件获取 page_refresh_interval，因为它是静态配置
    let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);

    Json(json!({
        "indices": indices,
        "stocks": stocks,
        "data_fetch_interval": data_interval,
        "page_refresh_interval": page_interval
    }))
}

// 新增：更新配置接口
async fn update_config(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let mut updated_data_interval = state.refresh_interval.read().await.clone();
    let mut updated_page_interval = 3; // 默认值
    
    // 尝试从当前配置文件中获取旧的 page_interval，以防本次只更新 data_interval
    if let Some(cfg) = load_config_from_file() {
        updated_page_interval = cfg.page_refresh_interval;
    }

    if let Some(val) = payload.get("data_fetch_interval").and_then(|v| v.as_u64()) {
        if val < 1 {
            return Json(json!({"status": "error", "msg": "data_fetch_interval must be >= 1"}));
        }
        updated_data_interval = val;
    }

    if let Some(val) = payload.get("page_refresh_interval").and_then(|v| v.as_u64()) {
        if val < 1 {
            return Json(json!({"status": "error", "msg": "page_refresh_interval must be >= 1"}));
        }
        updated_page_interval = val;
    }
    
    // 更新内存中的状态 (仅 data_fetch_interval 需要内存状态用于后端循环)
    {
        let mut current_interval = state.refresh_interval.write().await;
        *current_interval = updated_data_interval;
    }
    
    // 持久化保存
    let indices = state.index_list.read().await.clone();
    let stocks = state.stock_list.read().await.clone();
    let ai_configs = state.ai_configs.read().await.clone();
    save_config_to_file(&indices, &stocks, updated_data_interval, updated_page_interval, &ai_configs);
    
    println!("✅ Config updated: data_fetch={}s, page_refresh={}s", updated_data_interval, updated_page_interval);
    return Json(json!({"status": "success"}));
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
        
        // 修改：构建专用 Client，强制 HTTP/1.1 并增强 Header
        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "User-Agent", 
                    reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                );
                headers.insert(
                    "Referer", 
                    reqwest::header::HeaderValue::from_static("https://finance.sina.com.cn/")
                );
                headers.insert(
                    "Accept",
                    reqwest::header::HeaderValue::from_static("*/*")
                );
                headers
            })
            .timeout(std::time::Duration::from_secs(10))
            .http1_only() // 新增：强制 HTTP/1.1
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let url = format!("http://hq.sinajs.cn/list={}", normalized_code);
        
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                     eprintln!("❌ Detail Fetch HTTP Error: {} for {}", resp.status(), normalized_code);
                     return Json(json!({"status": "error", "msg": format!("HTTP {}", resp.status())}));
                }
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
        
        let secid = if normalized_code.starts_with("sh") {
            format!("1.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("sz") {
            format!("0.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("bj") {
            format!("0.{}", &normalized_code[2..])
        } else {
            format!("0.{}", normalized_code)
        };

        // 策略调整：
        // 1. 优先使用 realtime 接口，获取盘中实时资金流向，响应更快且数据通常更可用。
        // 2. 备用 daykline 接口，用于获取每日汇总数据，作为实时接口失败时的兜底。
        
        let urls = vec![
            // 首选：实时资金流向
            format!(
                "http://push2.eastmoney.com/api/qt/stock/fflow/realtime/get?secid={}&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65&ut=b2884a393a59ad64002292a3e90d46a5",
                secid
            ),
            // 备用：日K线资金流向 (取最近1天)
            format!(
                "http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get?secid={}&lmt=1&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65&ut=b2884a393a59ad64002292a3e90d46a5",
                secid
            )
        ];

        // 修复：构建更健壮的 Client，解决 "connection closed before message completed" 错误
        // 1. pool_max_idle_per_host(0): 禁用连接池空闲连接重用
        // 2. http1_only(): 强制使用 HTTP/1.1
        // 3. Connection: close header: 强制服务端在响应后关闭连接，彻底避免复用问题
        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
                headers.insert("Referer", reqwest::header::HeaderValue::from_static("http://quote.eastmoney.com/"));
                headers.insert("Accept", reqwest::header::HeaderValue::from_static("*/*"));
                headers.insert("Accept-Language", reqwest::header::HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8"));
                // 关键修复：强制关闭连接，不保持 Keep-Alive
                headers.insert("Connection", reqwest::header::HeaderValue::from_static("close"));
                headers
            })
            .pool_max_idle_per_host(0) // 关键修复：禁用连接池重用
            .http1_only()              // 关键修复：强制 HTTP/1.1
            .timeout(std::time::Duration::from_secs(15)) // 增加超时时间至15秒
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        for (idx, url) in urls.iter().enumerate() {
            // 增加简单重试逻辑，应对偶尔的网络抖动
            let mut retries = 2;
            let mut success = false;
            let mut last_error = None;

            while retries >= 0 && !success {
                match client.get(url).send().await {
                    Ok(resp) => {
                        if let Ok(text) = resp.text().await {
                            // 改进解析逻辑：查找第一个 '{' 和最后一个 '}'
                            if let Some(start_idx) = text.find('{') {
                                if let Some(end_idx) = text.rfind('}') {
                                    let json_str = &text[start_idx..=end_idx];
                                    
                                    if let Ok(root) = serde_json::from_str::<Value>(json_str) {
                                        if let Some(data) = root.get("data") {
                                            // 检查 data 是否为 null
                                            if data.is_null() {
                                                eprintln!("⚠️ EastMoney MoneyFlow data is null for {} (Attempt {}). This usually means the stock is suspended or no data available today.", normalized_code, idx + 1);
                                                break; // 数据为null，重试无用，尝试下一个接口
                                            }

                                            // --- 解析逻辑 ---
                                            
                                            // 情况 A: realtime 接口 (数据结构: data 直接包含 f62, f63 等字段)
                                            // 注意：realtime 接口返回结构可能因版本而异，通常 f62=主力, f63=超大, f64=大单, f65=中单, f66=小单
                                            // 这里尝试直接读取 data 下的字段
                                            if data.get("f62").is_some() {
                                                let main_net = data.get("f62").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                let super_large = data.get("f63").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                let large = data.get("f64").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                let medium = data.get("f65").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                let small = data.get("f66").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                
                                                // 有些 realtime 接口返回的是累计值或瞬时值，单位可能不同，这里假设单位一致(元)
                                                // 如果数据全为0，可能是停牌或未开盘
                                                
                                                return Json(json!({
                                                                    "status": "success",
                                                                    "data": {
                                                                        "main_net": main_net,
                                                                        "super_large": super_large,
                                                                        "large": large,
                                                                        "medium": medium,
                                                                        "small": small,
                                                                        "retail": small
                                                                    }
                                                                }));
                                            }

                                            // 情况 B: daykline 接口 (数据结构: data.klines[0] = "date,main,super,large,medium,small...")
                                            if let Some(klines) = data.get("klines") {
                                                if let Some(arr) = klines.as_array() {
                                                    if !arr.is_empty() {
                                                        if let Some(latest_str) = arr.first().and_then(|v| v.as_str()) {
                                                            let parts: Vec<&str> = latest_str.split(',').collect();
                                                            // 确保有足够的字段: Date, MainNet, SuperLarge, Large, Medium, Small
                                                            if parts.len() > 5 {
                                                                let main_net = parts[1].parse::<f64>().unwrap_or(0.0);
                                                                let super_large = parts[2].parse::<f64>().unwrap_or(0.0);
                                                                let large = parts[3].parse::<f64>().unwrap_or(0.0);
                                                                let medium = parts[4].parse::<f64>().unwrap_or(0.0);
                                                                let small = parts[5].parse::<f64>().unwrap_or(0.0);
                                                                
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
                                        } else {
                                            eprintln!("EastMoney MoneyFlow no data field");
                                        }
                                    } else {
                                        eprintln!("Failed to parse JSON from EastMoney: {}", json_str.chars().take(100).collect::<String>());
                                    }
                                }
                            }
                        }
                        success = true; // 请求成功且处理完毕（无论是否有数据），跳出重试循环
                    }
                    Err(e) => {
                        last_error = Some(e);
                        // 详细记录错误类型，帮助诊断
                        let error_type = if last_error.as_ref().unwrap().is_timeout() {
                            "Timeout"
                        } else if last_error.as_ref().unwrap().is_connect() {
                            "Connect"
                        } else if last_error.as_ref().unwrap().is_request() {
                            "Request"
                        } else {
                            "Unknown"
                        };
                        eprintln!("❌ Fetch money flow error (Type: {}, Attempt {}, Retry {}): {:?} for URL: {}", error_type, idx + 1, 2 - retries, last_error.as_ref().unwrap(), url);
                        
                        retries -= 1;
                        if retries >= 0 {
                            // 短暂等待后重试，增加等待时间以避开可能的限流
                            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                        }
                    }
                }
            }
            
            // 如果当前接口所有重试都失败，继续尝试下一个接口
            if let Some(e) = last_error {
                 eprintln!("⚠️ Interface {} failed after retries: {:?}", idx + 1, e);
            }
        }
        
        // 如果所有接口都失败
        Json(json!({
            "status": "success",
            "msg": "暂无资金流向数据",
            "data": null
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
            let data_interval = state.refresh_interval.read().await.clone();
            let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
            let ai_configs = state.ai_configs.read().await.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks, data_interval, page_interval, &ai_configs);
            
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
            let data_interval = state.refresh_interval.read().await.clone();
            let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
            let ai_configs = state.ai_configs.read().await.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks, data_interval, page_interval, &ai_configs);
            
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
        
        // 验证：确保新列表中的股票都是合法的且没有重复（可选，但建议）
        // 这里直接更新
        *list = new_codes.clone();
        println!("✅ Backend: Stocks reordered to {:?}", new_codes);
        
        // 保存配置
        let indices = state.index_list.read().await.clone();
        let data_interval = state.refresh_interval.read().await.clone();
        let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
        let ai_configs = state.ai_configs.read().await.clone();
        drop(list); // 释放写锁
        
        // 确保写入文件
        save_config_to_file(&indices, &new_codes, data_interval, page_interval, &ai_configs);
        
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "success"}));
    }
    Json(json!({"status": "error", "msg": "invalid payload"}))
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
            let data_interval = state.refresh_interval.read().await.clone();
            let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
            let ai_configs = state.ai_configs.read().await.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks, data_interval, page_interval, &ai_configs);
            
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "added"}));
        }
        state.data_refresh_notify.notify_one();
        return Json(json!({"status": "exists", "msg": "already exists"}));
    }
    Json(json!({"status": "error", "msg": "no code"}))
}

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
            let data_interval = state.refresh_interval.read().await.clone();
            let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
            let ai_configs = state.ai_configs.read().await.clone();
            drop(list); // 释放写锁
            save_config_to_file(&indices, &stocks, data_interval, page_interval, &ai_configs);
            
            state.data_refresh_notify.notify_one();
            return Json(json!({"status": "success", "msg": "removed"}));
        }
        return Json(json!({"status": "error", "msg": "not found"}));
    }
    Json(json!({"status": "error", "msg": "invalid payload"}))
}

async fn fetch_realtime_data(state: AppState, notify: Arc<Notify>) {
    // 修改：构建更健壮的 Client，强制 HTTP/1.1，增加更完整的 Header
    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "User-Agent", 
                reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            );
            headers.insert(
                "Referer", 
                reqwest::header::HeaderValue::from_static("https://finance.sina.com.cn/")
            );
            headers.insert(
                "Accept",
                reqwest::header::HeaderValue::from_static("*/*")
            );
            headers
        })
        .timeout(std::time::Duration::from_secs(10))
        .http1_only() // 新增：强制 HTTP/1.1
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    loop {
        let indices = state.index_list.read().await.clone();
        let stocks = state.stock_list.read().await.clone();

        let mut all_codes = indices.clone();
        all_codes.extend(stocks.iter().cloned());

        let mut market_data = vec![];

        for code in all_codes {
            let url = format!("http://hq.sinajs.cn/list={}", code);

            match client.get(&url).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        eprintln!("❌ Fetch HTTP Error: {} for {}", resp.status(), code);
                        continue;
                    }
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

                                market_data.push(StockInfo {
                                    symbol: code,
                                    name: parts[0].to_string(),
                                    price,
                                    change_percent,
                                    high,
                                    low,
                                    volume,
                                    open,
                                    pre_close,
                                    limit_up,
                                    limit_down,
                                    amount,
                                    turnover_rate: 0.0,
                                    pe_ratio: 0.0,
                                });
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Fetch error: {}", e),
            }
        }

        {
            let mut data = state.market_data.write().await;
            *data = market_data;
        }

        notify.notify_one();

        let interval = state.refresh_interval.read().await.clone();
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
    }
}

// 新增：获取 AI 配置
async fn get_ai_config(State(state): State<AppState>) -> Json<Value> {
    let configs = state.ai_configs.read().await.clone();
    Json(json!({
        "status": "success",
        "data": configs
    }))
}

// 新增：更新 AI 配置
async fn update_ai_config(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    if let Some(new_configs_val) = payload.get("ai_configs") {
        if let Some(new_configs) = new_configs_val.as_array() {
            let parsed_configs: Result<Vec<AiConfig>, _> = new_configs.iter().map(|v| {
                serde_json::from_value(v.clone())
            }).collect();

            match parsed_configs {
                Ok(configs) => {
                    // 更新内存状态
                    {
                        let mut ai_state = state.ai_configs.write().await;
                        *ai_state = configs.clone();
                    }
                    
                    // 持久化保存
                    let indices = state.index_list.read().await.clone();
                    let stocks = state.stock_list.read().await.clone();
                    let data_interval = state.refresh_interval.read().await.clone();
                    let page_interval = load_config_from_file().map_or(3, |c| c.page_refresh_interval);
                    
                    save_config_to_file(&indices, &stocks, data_interval, page_interval, &configs);
                    
                    return Json(json!({"status": "success"}));
                },
                Err(e) => {
                    return Json(json!({"status": "error", "msg": format!("Invalid config format: {}", e)}));
                }
            }
        }
    }
    Json(json!({"status": "error", "msg": "Invalid payload"}))
}

async fn get_stock_minute_data(
    Query(params): Query<HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        let datalen = params.get("datalen").unwrap_or(&"240".to_string()).parse::<usize>().unwrap_or(240);

        // 转换代码格式为东方财富接口所需格式
        let secid = if normalized_code.starts_with("sh") {
            format!("1.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("sz") {
            format!("0.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("bj") {
            format!("0.{}", &normalized_code[2..])
        } else {
            format!("0.{}", normalized_code)
        };

        // 新增：计算需要请求的天数 (ndays)
        // 目标：始终获取最近两个交易日的数据
        // 周一 (0): 需要上周五 + 周一。由于中间隔了周六日，ndays=3 才能涵盖周五到周一的时间跨度
        // 周二 (1): 需要周一 + 周二。连续交易日，ndays=2
        // 周三 (2): 需要周二 + 周三。连续交易日，ndays=2
        // 周四 (3): 需要周三 + 周四。连续交易日，ndays=2
        // 周五 (4): 需要周四 + 周五。连续交易日，ndays=2
        // 周末 (5,6): 通常不开盘，若查询则看最近一天或两天，这里统一处理为2天以获取周五数据
        
        let now = chrono::Local::now();
        let weekday = now.weekday().num_days_from_monday(); // Monday = 0, Sunday = 6
        
        let ndays = if weekday == 0 {
            3 // 周一：取3天以覆盖上周五
        } else {
            2 // 其他交易日：取2天以覆盖昨天和今天
        };

        let url = format!(
            "http://push2.eastmoney.com/api/qt/stock/trends2/get?secid={}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut=fa5fd1943c7b386f172d6893dbfba10b&ndays={}&iscr=0&iscca=0&datalen={}",
            secid, ndays, datalen
        );

        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
                headers.insert("Referer", reqwest::header::HeaderValue::from_static("http://quote.eastmoney.com/"));
                headers
            })
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        match client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(text) = resp.text().await {
                    // 解析 JSON
                    if let Ok(root) = serde_json::from_str::<Value>(&text) {
                        if let Some(data) = root.get("data") {
                            // 新增：检查 data 是否为 null
                            if data.is_null() {
                                eprintln!("⚠️ EastMoney Minute Data is null for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                                return Json(json!({"status": "error", "msg": "数据源无返回"}));
                            }

                            if let Some(trends) = data.get("trends") {
                                if let Some(arr) = trends.as_array() {
                                    let mut minute_data: Vec<MinuteDataPoint> = Vec::new();
                                    
                                    // 获取交易日期并清洗格式
                                    let raw_trade_date = data.get("tradeDate").and_then(|v| v.as_str()).unwrap_or("");
                                    // 清洗 tradeDate: 只保留 YYYY-MM-DD 部分，防止包含时间或其他字符
                                    // 兼容格式: "2023-10-27 15:00:00" 或 "2023-10-27"
                                    let trade_date = if raw_trade_date.len() >= 10 {
                                        raw_trade_date[..10].to_string()
                                    } else {
                                        raw_trade_date.to_string()
                                    };

                                    println!("Debug: tradeDate raw='{}', cleaned='{}'", raw_trade_date, trade_date);

                                    for item in arr {
                                        if let Some(s) = item.as_str() {
                                            let parts: Vec<&str> = s.split(',').collect();
                                            // f51:时间 (HH:MM), f52:价格, f53:均价, f54:成交量, f55:成交额
                                            if parts.len() >= 5 {
                                                let time_str_raw = parts[0].trim(); 
                                                let price = parts[1].parse::<f64>().unwrap_or(0.0);
                                                let avg_price = parts[2].parse::<f64>().unwrap_or(0.0);
                                                let volume = parts[3].parse::<f64>().unwrap_or(0.0); 
                                                
                                                // 修复：构建严格的标准时间字符串 "YYYY-MM-DD HH:mm"
                                                let full_time = if !trade_date.is_empty() && !time_str_raw.is_empty() {
                                                    // 只取前5位字符作为 HH:MM，防止包含秒数 (如 "09:30:00" -> "09:30")
                                                    let clean_time = if time_str_raw.len() >= 5 {
                                                        time_str_raw[..5].to_string()
                                                    } else {
                                                        time_str_raw.to_string()
                                                    };
                                                    format!("{} {}", trade_date, clean_time)
                                                } else if !time_str_raw.is_empty() {
                                                    // 如果没有 tradeDate，使用当前日期（仅在极端情况下）
                                                    let today = chrono::Local::now().format("%Y-%m-%d");
                                                    let clean_time = if time_str_raw.len() >= 5 {
                                                        time_str_raw[..5].to_string()
                                                    } else {
                                                        time_str_raw.to_string()
                                                    };
                                                    format!("{} {}", today, clean_time)
                                                } else {
                                                    continue; 
                                                };

                                                minute_data.push(MinuteDataPoint {
                                                    time: full_time,
                                                    price,
                                                    avg_price,
                                                    volume,
                                                    open: price, 
                                                    close: price,
                                                });
                                            }
                                        }
                                    }
                                    
                                    // 新增：如果解析后数据为空，但数组不为空（理论上不可能，除非所有行格式都错），打印日志
                                    if minute_data.is_empty() && !arr.is_empty() {
                                        eprintln!("⚠️ Parsed minute data is empty despite non-empty array for {}. Sample raw data: {}", normalized_code, arr.first().map_or("N/A", |v| v.as_str().unwrap_or("N/A")));
                                    }

                                    println!("Debug: Parsed {} minute data points for {}", minute_data.len(), normalized_code);
                                    return Json(json!({
                                        "status": "success",
                                        "data": minute_data
                                    }));
                                }
                            } else {
                                // 新增：trends 字段缺失
                                eprintln!("⚠️ No 'trends' field in response for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                            }
                        } else {
                             // 新增：data 字段缺失
                             eprintln!("⚠️ No 'data' field in response for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                        }
                    } else {
                         eprintln!("❌ Failed to parse JSON for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                    }
                }
            }
            Err(e) => eprintln!("Fetch minute data error: {}", e)
        }
    }
    Json(json!({"status": "error", "msg": "fetch failed"}))
}

// 新增：从东方财富获取K线数据
async fn fetch_kline_data_from_em(code: &str, klt: &str, limit: usize) -> Option<Vec<KLineDataPoint>> {
    let secid = if code.starts_with("sh") {
        format!("1.{}", &code[2..])
    } else if code.starts_with("sz") {
        format!("0.{}", &code[2..])
    } else if code.starts_with("bj") {
        format!("0.{}", &code[2..])
    } else {
        format!("0.{}", code)
    };

    // 东方财富K线接口
    let url = format!(
        "http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt={}&fqt=1&beg=0&end=20500101&lmt={}",
        secid, klt, limit
    );

    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
            headers.insert("Referer", reqwest::header::HeaderValue::from_static("http://quote.eastmoney.com/"));
            headers
        })
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    match client.get(&url).send().await {
        Ok(resp) => {
            if let Ok(text) = resp.text().await {
                if let Ok(root) = serde_json::from_str::<Value>(&text) {
                    if let Some(data) = root.get("data") {
                        if let Some(klines) = data.get("klines") {
                            if let Some(arr) = klines.as_array() {
                                let mut result = Vec::new();
                                for item in arr {
                                    if let Some(s) = item.as_str() {
                                        let parts: Vec<&str> = s.split(',').collect();
                                        // f51:日期, f52:开盘, f53:收盘, f54:最高, f55:最低, f56:成交量, f57:成交额, f58:振幅, f59:涨跌幅, f60:涨跌额, f61:换手率
                                        if parts.len() >= 7 {
                                            let date = parts[0];
                                            let open = parts[1].parse::<f64>().unwrap_or(0.0);
                                            let close = parts[2].parse::<f64>().unwrap_or(0.0);
                                            let high = parts[3].parse::<f64>().unwrap_or(0.0);
                                            let low = parts[4].parse::<f64>().unwrap_or(0.0);
                                            let volume = parts[5].parse::<f64>().unwrap_or(0.0);
                                            let amount = parts[6].parse::<f64>().unwrap_or(0.0);

                                            result.push(KLineDataPoint {
                                                date: date.to_string(),
                                                open,
                                                high,
                                                low,
                                                close,
                                                volume,
                                                amount,
                                            });
                                        }
                                    }
                                }
                                return if result.is_empty() { None } else { Some(result) };
                            }
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("Fetch EM KLine Error: {}", e)
    }
    None
}

async fn get_stock_kline_data(
    Query(params): Query<HashMap<String, String>>,
    _state: State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        let ktype = params.get("type").map(|s| s.as_str()).unwrap_or("kday");
        
        // 映射前端类型到东方财富 klt 参数及限制条数
        // 101=日K, 102=周K, 103=月K
        // 需求：默认显示90条数据
        let (klt, limit) = match ktype {
            "kweek" => ("102", 90),  // 周K取90周
            "kmonth" => ("103", 90), // 月K取90月
            _ => ("101", 90),       // 日K取90天
        };

        if let Some(data) = fetch_kline_data_from_em(&normalized_code, klt, limit).await {
            return Json(json!({
                "status": "success",
                "data": data
            }));
        }
    }
    Json(json!({"status": "error", "msg": "fetch failed or no data"}))
}

fn parse_sina_data(text: &str) -> Vec<StockInfo> {
    let mut result = Vec::new();
    for cap in RE_SINA_DATA.captures_iter(text) {
        let symbol = cap[1].to_string();
        let data_str = &cap[2];
        let parts: Vec<&str> = data_str.split(',').collect();
        
        // 确保有足够的数据字段
        if parts.len() < 32 {
            continue;
        }

        // 解析基本字段
        let name = parts[0].to_string();
        let open = parts[1].parse::<f64>().unwrap_or(0.0);
        let pre_close = parts[2].parse::<f64>().unwrap_or(0.0);
        let price = parts[3].parse::<f64>().unwrap_or(0.0);
        let high = parts[4].parse::<f64>().unwrap_or(0.0);
        let low = parts[5].parse::<f64>().unwrap_or(0.0);
        
        // 成交量(股) 和 成交额(元)
        let volume_shares = parts[8].parse::<f64>().unwrap_or(0.0);
        let amount = parts[9].parse::<f64>().unwrap_or(0.0);
        
        // 转换成交量为手 (1手=100股)
        let volume_hands = volume_shares / 100.0;

        let change_percent = if pre_close > 0.0 { ((price - pre_close) / pre_close) * 100.0 } else { 0.0 };
        
        // 计算涨跌停价 (简化处理，实际应根据板块判断)
        let limit_up = pre_close * 1.1;
        let limit_down = pre_close * 0.9;

        result.push(StockInfo {
            symbol,
            name,
            price,
            change_percent,
            high,
            low,
            volume: volume_hands,
            open,
            pre_close,
            limit_up,
            limit_down,
            amount,
            turnover_rate: 0.0, // 新浪接口此位置可能不同，暂置0
            pe_ratio: 0.0,      // 暂置0
        });
    }
    result
}
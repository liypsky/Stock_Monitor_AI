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
use chrono::Timelike;

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
    #[serde(default)]
    pub amount: f64, // 新增：成交额(元)
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

// 新增：缓存条目结构
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    data: T,
    timestamp: std::time::Instant,
}

// 新增：缓存类型定义
type DetailCache = Arc<RwLock<HashMap<String, CacheEntry<StockInfo>>>>;
type MoneyFlowCache = Arc<RwLock<HashMap<String, CacheEntry<MoneyFlow>>>>;
type MinuteDataCache = Arc<RwLock<HashMap<String, CacheEntry<Vec<MinuteDataPoint>>>>>;
type KLineDataCache = Arc<RwLock<HashMap<String, CacheEntry<Vec<KLineDataPoint>>>>>;

#[derive(Clone)]
struct AppState {
    index_list: Arc<RwLock<Vec<String>>>,
    stock_list: Arc<RwLock<Vec<String>>>,
    market_data: Arc<RwLock<Vec<StockInfo>>>,
    data_refresh_notify: Arc<Notify>,
    refresh_interval: Arc<RwLock<u64>>, // 新增：刷新间隔
    ai_configs: Arc<RwLock<Vec<AiConfig>>>, // 新增：AI配置
    // 新增：缓存字段
    detail_cache: DetailCache,
    money_flow_cache: MoneyFlowCache,
    minute_data_cache: MinuteDataCache,
    kline_data_cache: KLineDataCache,
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
        // 新增：初始化缓存
        detail_cache: Arc::new(RwLock::new(HashMap::new())),
        money_flow_cache: Arc::new(RwLock::new(HashMap::new())),
        minute_data_cache: Arc::new(RwLock::new(HashMap::new())),
        kline_data_cache: Arc::new(RwLock::new(HashMap::new())),
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

// 新增：获取个股详情 (带缓存)
async fn get_stock_detail(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        
        // 1. 尝试从缓存获取 (有效期 60 秒)
        {
            let cache = state.detail_cache.read().await;
            if let Some(entry) = cache.get(&normalized_code) {
                if entry.timestamp.elapsed().as_secs() < 60 {
                    // 缓存命中
                    return Json(json!({
                        "status": "success",
                        "data": {
                            "symbol": entry.data.symbol,
                            "name": entry.data.name,
                            "price": entry.data.price,
                            "pre_close": entry.data.pre_close,
                            "open": entry.data.open,
                            "high": entry.data.high,
                            "low": entry.data.low,
                            "volume": entry.data.volume,
                            "amount": entry.data.amount,
                            "limit_up": entry.data.limit_up,
                            "limit_down": entry.data.limit_down,
                            "change_percent": entry.data.change_percent
                        },
                        "cached": true
                    }));
                }
            }
        }

        // 2. 缓存未命中或过期，从网络获取
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

                            let stock_info = StockInfo {
                                symbol: normalized_code.clone(),
                                name: parts[0].to_string(),
                                price,
                                pre_close,
                                open,
                                high,
                                low,
                                volume: volume / 100.0, // 转换为手
                                amount,
                                limit_up,
                                limit_down,
                                change_percent,
                                turnover_rate: 0.0,
                                pe_ratio: 0.0,
                            };

                            // 3. 更新缓存
                            {
                                let mut cache = state.detail_cache.write().await;
                                cache.insert(normalized_code.clone(), CacheEntry {
                                    data: stock_info.clone(),
                                    timestamp: std::time::Instant::now(),
                                });
                            }

                            return Json(json!({
                                "status": "success",
                                "data": {
                                    "symbol": stock_info.symbol,
                                    "name": stock_info.name,
                                    "price": stock_info.price,
                                    "pre_close": stock_info.pre_close,
                                    "open": stock_info.open,
                                    "high": stock_info.high,
                                    "low": stock_info.low,
                                    "volume": stock_info.volume,
                                    "amount": stock_info.amount,
                                    "limit_up": stock_info.limit_up,
                                    "limit_down": stock_info.limit_down,
                                    "change_percent": stock_info.change_percent
                                },
                                "cached": false
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

// 新增：获取资金流向 (带缓存)
async fn get_stock_money_flow(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        
        // 1. 尝试从缓存获取 (有效期 300 秒，资金流向变化相对较慢)
        {
            let cache = state.money_flow_cache.read().await;
            if let Some(entry) = cache.get(&normalized_code) {
                if entry.timestamp.elapsed().as_secs() < 300 {
                    return Json(json!({
                        "status": "success",
                        "data": entry.data,
                        "cached": true
                    }));
                }
            }
        }

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
                                                
                                                let mf_data = MoneyFlow {
                                                    main_net,
                                                    super_large,
                                                    large,
                                                    medium,
                                                    small,
                                                    retail: small,
                                                };

                                                // 2. 更新缓存
                                                {
                                                    let mut cache = state.money_flow_cache.write().await;
                                                    cache.insert(normalized_code.clone(), CacheEntry {
                                                        data: mf_data.clone(),
                                                        timestamp: std::time::Instant::now(),
                                                    });
                                                }
                                                
                                                return Json(json!({
                                                                    "status": "success",
                                                                    "data": mf_data,
                                                                    "cached": false
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

                                                                let mf_data = MoneyFlow {
                                                                    main_net,
                                                                    super_large,
                                                                    large,
                                                                    medium,
                                                                    small,
                                                                    retail,
                                                                };

                                                                // 2. 更新缓存
                                                                {
                                                                    let mut cache = state.money_flow_cache.write().await;
                                                                    cache.insert(normalized_code.clone(), CacheEntry {
                                                                        data: mf_data.clone(),
                                                                        timestamp: std::time::Instant::now(),
                                                                    });
                                                                }

                                                                return Json(json!({
                                                                    "status": "success",
                                                                    "data": mf_data,
                                                                    "cached": false
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

// 新增：添加指数
async fn add_index(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        // 指数代码通常已经是 sh/sz/bj 格式，或者需要简单标准化
        let normalized_code = if code.starts_with("sh") || code.starts_with("sz") || code.starts_with("bj") {
            code.to_lowercase()
        } else {
            // 默认假设是上海或深圳，根据常见指数代码前缀判断，这里简化处理，直接使用前缀或默认sh
            if code.starts_with("0") || code.starts_with("3") || code.starts_with("1") || code.starts_with("2") {
                 format!("sz{}", code.to_lowercase())
            } else {
                 format!("sh{}", code.to_lowercase())
            }
        };

        let mut list = state.index_list.write().await;
        
        if !list.contains(&normalized_code) {
            list.push(normalized_code.clone());
            println!("✅ Backend: Added index {}", normalized_code);
            
            // 保存配置
            let indices = list.clone();
            let stocks = state.stock_list.read().await.clone();
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

// 新增：删除指数
#[axum::debug_handler]
async fn remove_index(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    if let Some(code) = payload.get("code").and_then(|c| c.as_str()) {
        let normalized_code = if code.starts_with("sh") || code.starts_with("sz") || code.starts_with("bj") {
            code.to_lowercase()
        } else {
             if code.starts_with("0") || code.starts_with("3") || code.starts_with("1") || code.starts_with("2") {
                 format!("sz{}", code.to_lowercase())
            } else {
                 format!("sh{}", code.to_lowercase())
            }
        };

        let mut list = state.index_list.write().await;
        if let Some(pos) = list.iter().position(|x| x == &normalized_code) {
            list.remove(pos);
            println!("✅ Backend: Removed index {}", normalized_code);
            
            // 保存配置
            let indices = list.clone();
            let stocks = state.stock_list.read().await.clone();
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

// 新增：AI 请求结构体，用于接收前端传来的上下文或直接由后端构建
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAnalysisRequest {
    pub code: String,
    #[serde(default)]
    pub analysis_type: String, // trend, timing, turning
}

// 新增：AI 响应结构体，包含分析结果和建议方向
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAnalysisResponse {
    pub status: String,
    pub analysis: String,
    #[serde(default)]
    pub sentiment: String, // "bullish", "bearish", "neutral"
}

// 新增：调用大模型的核心函数
async fn call_llm_api(config: &AiConfig, prompt: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.timeout_seconds))
        .build()
        .map_err(|e| format!("Failed to build client: {}", e))?;

    // 构建请求 Body，兼容 OpenAI 格式
    let request_body = json!({
        "model": config.model,
        "messages": [
            {"role": "system", "content": "你是一个专业的A股金融分析师。请根据提供的数据进行分析。输出格式要求：先给出结论（看多/看空/震荡），然后给出详细理由。如果在结论中包含'看多'或'买入'，请在开头标记[BULLISH]；如果包含'看空'或'卖出'，标记[BEARISH]；否则标记[NEUTRAL]。"}
            , {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    });

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    if !config.api_key.is_empty() {
        headers.insert("Authorization", format!("Bearer {}", config.api_key).parse().unwrap());
    }

    let resp = client.post(&config.api_url)
        .headers(headers)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("API Error {}: {}", status, text));
    }

    let json_resp: Value = resp.json().await.map_err(|e| format!("Parse response failed: {}", e))?;
    
    // 解析 OpenAI 格式响应
    if let Some(choices) = json_resp.get("choices") {
        if let Some(first_choice) = choices.as_array().and_then(|arr| arr.first()) {
            if let Some(message) = first_choice.get("message") {
                if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                    return Ok(content.to_string());
                }
            }
        }
    }
    
    Err("Invalid response format from LLM".to_string())
}

// 新增：调用大模型的核心函数，支持指定模型名称
async fn call_llm_api_with_model(config: &AiConfig, prompt: &str, model_name: &str) -> Result<String, String> {
    // 修改：获取超时时间，默认为 60 秒
    let timeout_secs = if config.timeout_seconds > 0 { config.timeout_seconds } else { 60 };
    
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| format!("Failed to build client: {}", e))?;

    // 智能补全 API URL
    // 大多数 OpenAI 兼容接口（包括 OpenRouter, Ollama OpenAI 兼容层）的聊天端点都是 /chat/completions
    let mut final_url = config.api_url.clone();
    if !final_url.ends_with("/chat/completions") {
        if final_url.ends_with('/') {
            final_url.push_str("chat/completions");
        } else {
            // 如果用户配置的是 https://openrouter.ai/api/v1，我们需要追加 /chat/completions
            // 如果用户配置的是 http://localhost:11434/v1，我们需要追加 /chat/completions
            // 简单判断：如果包含 /v1 但不以 /chat/completions 结尾，则追加
            if final_url.contains("/v1") {
                final_url.push_str("/chat/completions");
            } else {
                // 对于根路径或其他情况，也尝试追加，防止 404
                final_url.push_str("/chat/completions");
            }
        }
    }

    // 构建请求 Body，兼容 OpenAI 格式
    let request_body = json!({
        "model": model_name,
        "messages": [
            {"role": "system", "content": "你是一个专业的A股金融分析师。请根据提供的数据进行分析。输出格式要求：先给出结论（看多/看空/震荡），然后给出详细理由。如果在结论中包含'看多'或'买入'，请在开头标记[BULLISH]；如果包含'看空'或'卖出'，标记[BEARISH]；否则标记[NEUTRAL]。"}
            , {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    });

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    if !config.api_key.is_empty() {
        headers.insert("Authorization", format!("Bearer {}", config.api_key).parse().unwrap());
    }

    println!("🔗 Calling AI: URL={}, Model={}", final_url, model_name);

    let resp = client.post(&final_url)
        .headers(headers)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| format!("Request failed for model {}: {}", model_name, e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("API Error {} for model {}: {}", status, model_name, text));
    }

    let json_resp: Value = resp.json().await.map_err(|e| format!("Parse response failed for model {}: {}", model_name, e))?;
    
    // 解析 OpenAI 格式响应
    if let Some(choices) = json_resp.get("choices") {
        if let Some(first_choice) = choices.as_array().and_then(|arr| arr.first()) {
            if let Some(message) = first_choice.get("message") {
                if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                    return Ok(content.to_string());
                }
            }
        }
    }
    
    Err(format!("Invalid response format from LLM for model {}", model_name))
}

// 新增：构建 Prompt 的辅助函数
fn build_analysis_prompt(stock_info: &StockInfo, money_flow: Option<&MoneyFlow>, kline_data: &[KLineDataPoint], analysis_type: &str) -> String {
    let mut prompt = format!(
        "请分析股票 {}({}) 的走势。\n\n【基础数据】\n当前价格: {:.2}, 涨跌幅: {:.2}%, 今开: {:.2}, 最高: {:.2}, 最低: {:.2}, 昨收: {:.2}\n",
        stock_info.name, stock_info.symbol, stock_info.price, stock_info.change_percent, 
        stock_info.open, stock_info.high, stock_info.low, stock_info.pre_close
    );

    if let Some(mf) = money_flow {
        prompt.push_str(&format!(
            "\n【资金流向】\n主力净流入: {:.2}元, 超大单: {:.2}元, 大单: {:.2}元, 中单: {:.2}元, 小单: {:.2}元\n",
            mf.main_net, mf.super_large, mf.large, mf.medium, mf.small
        ));
    } else {
        prompt.push_str("\n【资金流向】\n暂无数据\n");
    }

    if !kline_data.is_empty() {
        prompt.push_str("\n【近期K线数据 (最近5日)】\n日期, 开盘, 收盘, 最高, 最低, 成交量(手)\n");
        for k in kline_data.iter().rev().take(5) {
            prompt.push_str(&format!("{}, {:.2}, {:.2}, {:.2}, {:.2}, {:.0}\n", k.date, k.open, k.close, k.high, k.low, k.volume));
        }
    }

    match analysis_type {
        "trend" => prompt.push_str("\n【分析任务】\n请进行趋势研判。结合价格形态、均线趋势（如有）、资金流向，判断短期和中期的走势。给出明确的看多、看空或震荡观点。"),
        "timing" => prompt.push_str("\n【分析任务】\n请提供择时信号。分析当前的买卖点，是否适合介入或离场？关注成交量变化和关键支撑压力位。"),
        "turning" => prompt.push_str("\n【分析任务】\n请检测拐点。分析是否有见底回升或见顶回落的迹象。关注背离现象和资金异动。"),
        _ => prompt.push_str("\n【分析任务】\n请进行综合技术分析。"),
    }

    prompt
}

// 修改：get_ai_analysis 改为 POST 以接收更多参数，或者保持 GET 但内部获取数据
// 这里为了兼容前端现有逻辑较少改动，我们保持 GET 入口，但内部实现完整逻辑
// 注意：原代码是 GET，我们将其逻辑重写
async fn get_ai_analysis(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    let code = params.get("code").cloned().unwrap_or_default();
    let analysis_type = params.get("type").cloned().unwrap_or("trend".to_string());

    if code.is_empty() {
        return Json(json!({"status": "error", "msg": "no code"}));
    }

    let normalized_code = normalize_stock_code(&code);

    // 1. 获取所有 AI 配置
    let ai_configs = state.ai_configs.read().await.clone();
    if ai_configs.is_empty() {
        return Json(json!({"status": "error", "msg": "No AI config found"}));
    }

    // 2. 并行获取股票数据 (模拟前端之前的行为，但在后端完成以保证数据一致性)
    // 获取详情
    let stock_info_opt = fetch_stock_detail_internal(&normalized_code).await;
    // 获取资金流
    let money_flow_opt = fetch_money_flow_internal(&normalized_code).await;
    // 获取K线
    // 获取K线
    let kline_data_opt = fetch_kline_data_from_em_internal(&normalized_code, "101", 10).await; // 取日K最近10条

    if stock_info_opt.is_none() {
        return Json(json!({"status": "error", "msg": "Failed to fetch stock info"}));
    }
    
    let stock_info = stock_info_opt.unwrap();
    let kline_data = kline_data_opt.unwrap_or_default();

    // 3. 构建 Prompt
    let prompt = build_analysis_prompt(&stock_info, money_flow_opt.as_ref(), &kline_data, &analysis_type);

    // 4. 调用 LLM - 遍历所有配置和模型
    let mut last_error = String::new();
    let mut analysis_result = None;
    let mut tried_count = 0;

    for config in &ai_configs {
        // 解析当前配置的模型列表
        let main_models = config.main_models.split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>();
        
        let fallback_models = config.fallback_models.split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>();

        // 合并模型列表：主模型 + 备用模型
        let mut all_models = main_models.clone();
        all_models.extend(fallback_models.clone());

        // 如果列表为空，使用默认 model 字段
        if all_models.is_empty() {
            if !config.model.is_empty() {
                all_models.push(&config.model);
            } else {
                eprintln!("⚠️ Config {} has no models configured, skipping.", config.name);
                continue;
            }
        }

        println!("🤖 Trying AI Provider: {} with models: {:?}", config.name, all_models);

        for model_name in all_models {
            tried_count += 1;
            println!("🤖 Attempting AI analysis with provider [{}] model: {}", config.name, model_name);
            match call_llm_api_with_model(config, &prompt, model_name).await {
                Ok(text) => {
                    analysis_result = Some(text);
                    println!("✅ AI analysis successful with provider [{}] model: {}", config.name, model_name);
                    break; // 成功则跳出模型循环
                }
                Err(e) => {
                    eprintln!("⚠️ AI Call Error with provider [{}] model {}: {}", config.name, model_name, e);
                    last_error = format!("Provider [{}] Model [{}]: {}", config.name, model_name, e);
                    // 继续尝试下一个模型
                }
            }
        }

        // 如果当前配置中已有模型成功，则跳出配置循环
        if analysis_result.is_some() {
            break;
        }
    }

    let final_analysis = match analysis_result {
        Some(text) => text,
        None => {
            let err_msg = if tried_count == 0 {
                "No valid models found in any configuration".to_string()
            } else {
                format!("All AI providers and models failed. Last error: {}", last_error)
            };
            return Json(json!({"status": "error", "msg": err_msg}));
        }
    };

    // 5. 解析情感倾向
    let mut sentiment = "neutral".to_string();
    let mut clean_analysis = final_analysis.clone();
    
    if final_analysis.contains("[BULLISH]") {
        sentiment = "bullish".to_string();
        clean_analysis = final_analysis.replace("[BULLISH]", "");
    } else if final_analysis.contains("[BEARISH]") {
        sentiment = "bearish".to_string();
        clean_analysis = final_analysis.replace("[BEARISH]", "");
    } else if final_analysis.contains("[NEUTRAL]") {
        sentiment = "neutral".to_string();
        clean_analysis = final_analysis.replace("[NEUTRAL]", "");
    }

    Json(json!({
        "status": "success",
        "data": {
            "analysis": clean_analysis.trim(),
            "sentiment": sentiment,
            "raw_response": final_analysis // 可选，用于调试
        }
    }))
}

// 新增：内部获取详情的辅助函数 (复用原有逻辑但不返回 Json)
async fn fetch_stock_detail_internal(code: &str) -> Option<StockInfo> {
    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
            headers.insert("Referer", reqwest::header::HeaderValue::from_static("https://finance.sina.com.cn/"));
            headers
        })
        .timeout(std::time::Duration::from_secs(10))
        .http1_only()
        .build()
        .ok()?;

    let url = format!("http://hq.sinajs.cn/list={}", code);
    let resp = client.get(&url).send().await.ok()?;
    let bytes = resp.bytes().await.ok()?;
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
            let volume_shares = parts[8].parse::<f64>().unwrap_or(0.0);
            let amount = parts[9].parse::<f64>().unwrap_or(0.0);
            let change_percent = if pre_close > 0.0 { ((price - pre_close) / pre_close) * 100.0 } else { 0.0 };
            
            return Some(StockInfo {
                symbol: code.to_string(),
                name: parts[0].to_string(),
                price,
                change_percent,
                high,
                low,
                volume: volume_shares / 100.0,
                open,
                pre_close,
                limit_up: pre_close * 1.1,
                limit_down: pre_close * 0.9,
                amount,
                turnover_rate: 0.0,
                pe_ratio: 0.0,
            });
        }
    }
    None
}

// 新增：内部获取资金流的辅助函数
async fn fetch_money_flow_internal(code: &str) -> Option<MoneyFlow> {
    // 简化版：只尝试 realtime 接口
    let secid = if code.starts_with("sh") {
        format!("1.{}", &code[2..])
    } else if code.starts_with("sz") || code.starts_with("bj") {
        format!("0.{}", &code[2..])
    } else {
        format!("0.{}", code)
    };

    let url = format!(
        "http://push2.eastmoney.com/api/qt/stock/fflow/realtime/get?secid={}&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65&ut=b2884a393a59ad64002292a3e90d46a5",
        secid
    );

    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
            headers.insert("Referer", reqwest::header::HeaderValue::from_static("http://quote.eastmoney.com/"));
            headers.insert("Connection", reqwest::header::HeaderValue::from_static("close"));
            headers
        })
        .pool_max_idle_per_host(0)
        .http1_only()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok()?;

    let resp = client.get(&url).send().await.ok()?;
    let text = resp.text().await.ok()?;
    
    if let Some(start_idx) = text.find('{') {
        if let Some(end_idx) = text.rfind('}') {
            let json_str = &text[start_idx..=end_idx];
            if let Ok(root) = serde_json::from_str::<Value>(json_str) {
                if let Some(data) = root.get("data") {
                    if !data.is_null() && data.get("f62").is_some() {
                        let main_net = data.get("f62").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let super_large = data.get("f63").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let large = data.get("f64").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let medium = data.get("f65").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let small = data.get("f66").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        
                        return Some(MoneyFlow {
                            main_net,
                            super_large,
                            large,
                            medium,
                            small,
                            retail: small,
                        });
                    }
                }
            }
        }
    }
    None
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
    State(state): State<AppState>,
) -> Json<Value> {
    if let Some(code) = params.get("code") {
        let normalized_code = normalize_stock_code(code);
        
        // 1. 尝试从缓存获取 (有效期 10 秒，分时图需要较高实时性，但也要防止频繁请求)
        {
            let cache = state.minute_data_cache.read().await;
            if let Some(entry) = cache.get(&normalized_code) {
                if entry.timestamp.elapsed().as_secs() < 10 {
                    return Json(json!({
                        "status": "success",
                        "data": entry.data,
                        "cached": true
                    }));
                }
            }
        }
        
        // 转换代码格式为东方财富格式 (例如 sh600519 -> 1.600519)
        let secid = if normalized_code.starts_with("sh") {
            format!("1.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("sz") {
            format!("0.{}", &normalized_code[2..])
        } else if normalized_code.starts_with("bj") {
            format!("0.{}", &normalized_code[2..])
        } else {
            format!("0.{}", normalized_code)
        };

        // 使用东方财富分时数据接口
        // fields2: f51(时间), f52(最新价), f53(均价), f54(成交量(手)), f55(成交额(元))...
        // isclose=1 表示包含收盘价，datalen=240 获取全天数据
        let url = format!(
            "http://push2.eastmoney.com/api/qt/stock/trends/get?secid={}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&isclose=1&datalen=240",
            secid
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
                        // 检查 rc 状态码，0 表示成功
                        if let Some(rc) = root.get("rc").and_then(|v| v.as_i64()) {
                            if rc != 0 {
                                eprintln!("⚠️ Eastmoney API returned error rc: {} for {}", rc, normalized_code);
                                return Json(json!({
                                    "status": "success",
                                    "data": [],
                                    "msg": "API Error"
                                }));
                            }
                        }

                        // 修复：更健壮地处理 data 字段结构
                        if let Some(data) = root.get("data") {
                            // 情况1: data 是数组。这通常发生在股票停牌、未上市或接口返回状态包时。
                            if data.is_array() {
                                let arr = data.as_array().unwrap();
                                
                                let mut minute_data: Vec<MinuteDataPoint> = Vec::new();
                                let mut trade_date_str = "";

                                for item in arr {
                                    if let Some(obj) = item.as_object() {
                                        let f2_val = obj.get("f2").and_then(|v| v.as_i64()).unwrap_or(0);
                                        if f2_val == 0 { continue; }
                                        
                                        let min_part = f2_val % 100;
                                        let hour_part = (f2_val / 100) % 100;
                                        let day_part = (f2_val / 10000) % 100;
                                        let month_part = (f2_val / 1000000) % 100;
                                        let year_part = (f2_val / 100000000) % 100;
                                        
                                        let full_year = 2000 + year_part;
                                        
                                        if trade_date_str.is_empty() {
                                            trade_date_str = Box::leak(format!("{:04}-{:02}-{:02}", full_year, month_part, day_part).into_boxed_str());
                                        }
                                        
                                        let time_str = format!("{:04}-{:02}-{:02} {:02}:{:02}", full_year, month_part, day_part, hour_part, min_part);
                                        
                                        let price = obj.get("f3").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                        let avg_price = price; 
                                        
                                        // 尝试从数组对象中获取成交额，如果存在 f5 或 f6 等字段
                                        // 注意：不同股票或接口版本可能字段不同，这里尽量兼容
                                        let amount = obj.get("f5").and_then(|v| v.as_f64()).unwrap_or(0.0);

                                        minute_data.push(MinuteDataPoint {
                                            time: time_str,
                                            price,
                                            avg_price,
                                            volume: 0.0,
                                            open: price,
                                            close: price,
                                            amount,
                                        });
                                    }
                                }

                                if !minute_data.is_empty() {
                                     // 2. 更新缓存
                                    {
                                        let mut cache = state.minute_data_cache.write().await;
                                        cache.insert(normalized_code.clone(), CacheEntry {
                                            data: minute_data.clone(),
                                            timestamp: std::time::Instant::now(),
                                        });
                                    }
                                    
                                    let now = chrono::Local::now();
                                    let weekday = now.weekday().num_days_from_monday();
                                    let hour = now.hour();
                                    let minute = now.minute();
                                    let time_val = hour * 60 + minute;
                                    let is_trading_time = weekday < 5 && (
                                        (time_val >= 9 * 60 + 30 && time_val < 11 * 60 + 30) ||
                                        (time_val >= 13 * 60 && time_val < 15 * 60)
                                    );

                                    return Json(json!({
                                        "status": "success",
                                        "data": minute_data,
                                        "trade_date": trade_date_str,
                                        "is_trading_time": is_trading_time,
                                        "cached": false,
                                        "msg": "Parsed from array format"
                                    }));
                                } else {
                                    eprintln!("⚠️ Eastmoney minute data array parsed but empty valid points for {}", normalized_code);
                                    return Json(json!({
                                        "status": "success",
                                        "data": [],
                                        "msg": "暂无有效分时数据"
                                    }));
                                }
                            }
                            
                            // 情况2: data 是对象，尝试获取 trends
                            if data.is_object() {
                                if let Some(trends) = data.get("trends") {
                                    if let Some(arr) = trends.as_array() {
                                        let mut minute_data: Vec<MinuteDataPoint> = Vec::new();
                                        let trade_date = data.get("tradeDate").and_then(|v| v.as_str()).unwrap_or("");

                                        for item in arr {
                                            if let Some(s) = item.as_str() {
                                                let parts: Vec<&str> = s.split(',').collect();
                                                // 东方财富分时数据格式: 
                                                // f51:时间(HH:MM), f52:最新价, f53:均价, f54:成交量(手), f55:成交额(元)...
                                                // 索引:      0         1       2       3           4
                                                if parts.len() >= 5 {
                                                    let time_str_raw = parts[0].trim();
                                                    let price = parts[1].parse::<f64>().unwrap_or(0.0);
                                                    let avg_price = parts[2].parse::<f64>().unwrap_or(0.0);
                                                    let volume = parts[3].parse::<f64>().unwrap_or(0.0); // 单位：手
                                                    // 新增：解析成交额 (parts[4])
                                                    let amount = parts[4].parse::<f64>().unwrap_or(0.0);
                                                    
                                                    let full_time = if !trade_date.is_empty() && !time_str_raw.is_empty() {
                                                        let clean_time = if time_str_raw.len() >= 5 {
                                                            time_str_raw[..5].to_string()
                                                        } else {
                                                            time_str_raw.to_string()
                                                        };
                                                        format!("{} {}", trade_date, clean_time)
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
                                                        amount, // 新增：赋值成交额
                                                    });
                                                }
                                            }
                                        }
                                        
                                        // 2. 更新缓存
                                        {
                                            let mut cache = state.minute_data_cache.write().await;
                                            cache.insert(normalized_code.clone(), CacheEntry {
                                                data: minute_data.clone(),
                                                timestamp: std::time::Instant::now(),
                                            });
                                        }
                                        
                                        let now = chrono::Local::now();
                                        let weekday = now.weekday().num_days_from_monday();
                                        let hour = now.hour();
                                        let minute = now.minute();
                                        let time_val = hour * 60 + minute;
                                        
                                        let is_trading_time = weekday < 5 && (
                                            (time_val >= 9 * 60 + 30 && time_val < 11 * 60 + 30) ||
                                            (time_val >= 13 * 60 && time_val < 15 * 60)
                                        );

                                        return Json(json!({
                                            "status": "success",
                                            "data": minute_data,
                                            "trade_date": trade_date,
                                            "is_trading_time": is_trading_time,
                                            "cached": false
                                        }));
                                    }
                                } else {
                                    eprintln!("⚠️ No 'trends' field in Eastmoney data object for {}. Keys: {:?}", normalized_code, data.as_object().map(|o| o.keys().collect::<Vec<_>>()));
                                    return Json(json!({
                                        "status": "success",
                                        "data": [],
                                        "msg": "数据格式异常 (无trends字段)"
                                    }));
                                }
                            }
                        } else {
                             eprintln!("⚠️ No 'data' field in Eastmoney response for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                        }
                    } else {
                         eprintln!("❌ Failed to parse Eastmoney minute JSON for {}. Response: {}", normalized_code, text.chars().take(200).collect::<String>());
                    }
                }
            }
            Err(e) => eprintln!("Fetch Eastmoney minute data error: {}", e)
        }
    }
    Json(json!({"status": "error", "msg": "fetch failed"}))
}

async fn fetch_kline_data_from_em_internal(code: &str, klt: &str, limit: usize) -> Option<Vec<KLineDataPoint>> {
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
    // klt: 101(日), 102(周), 103(月)
    // fqt: 1(前复权)
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
                // println!("Debug EM KLine Raw: {}", text.chars().take(200).collect::<String>());
                
                if let Ok(root) = serde_json::from_str::<Value>(&text) {
                    // 检查 rc 状态码
                    if let Some(rc) = root.get("rc").and_then(|v| v.as_i64()) {
                        if rc != 0 {
                            eprintln!("⚠️ Eastmoney KLine API returned error rc: {} for {}", rc, code);
                            return None;
                        }
                    }

                    if let Some(data) = root.get("data") {
                        // 如果 data 是空数组或 null，直接返回 None
                        if data.is_null() || (data.is_array() && data.as_array().unwrap().is_empty()) {
                            eprintln!("⚠️ Eastmoney KLine data is empty/null for {}", code);
                            return None;
                        }

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
                        } else {
                             eprintln!("⚠️ No 'klines' field in Eastmoney KLine data for {}", code);
                        }
                    } else {
                         eprintln!("⚠️ No 'data' field in Eastmoney KLine response for {}", code);
                    }
                } else {
                     eprintln!("❌ Failed to parse Eastmoney KLine JSON for {}", code);
                }
            }
        }
        Err(e) => eprintln!("Fetch EM KLine Error: {}", e)
    }
    None
}

async fn get_stock_kline_data(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
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

        // 生成缓存 Key，包含类型，因为不同周期的K线数据不同
        let cache_key = format!("{}_{}", normalized_code, klt);

        // 1. 尝试从缓存获取 (有效期 300 秒，K线数据变化频率较低)
        {
            let cache = state.kline_data_cache.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                if entry.timestamp.elapsed().as_secs() < 300 {
                    return Json(json!({
                        "status": "success",
                        "data": entry.data,
                        "cached": true
                    }));
                }
            }
        }

        // 2. 缓存未命中或过期，从网络获取
        if let Some(data) = fetch_kline_data_from_em_internal(&normalized_code, klt, limit).await {
            // 3. 更新缓存
            {
                let mut cache = state.kline_data_cache.write().await;
                cache.insert(cache_key, CacheEntry {
                    data: data.clone(),
                    timestamp: std::time::Instant::now(),
                });
            }
            return Json(json!({
                "status": "success",
                "data": data,
                "cached": false
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
use axum::{
    extract::{Query, State},
    routing::get,
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
// 引入 encoding_rs
use encoding_rs::GBK;

// 假设 StockInfo 结构体定义如下
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
}

#[derive(Clone)]
struct AppState {
    index_list: Arc<RwLock<Vec<String>>>,
    stock_list: Arc<RwLock<Vec<String>>>,
    market_data: Arc<RwLock<Vec<StockInfo>>>,
    // 新增：用于通知后台任务立即刷新数据
    data_refresh_notify: Arc<Notify>,
}

// 预编译正则表达式，提升性能
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
        .route("/api/add_stock", get(add_stock))
        .route("/api/add_index", get(add_index))
        .route("/api/config", get(get_config))
        .nest_service("/", ServeDir::new("static"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9527").await.unwrap();
    println!("🚀 A股实时监控服务已启动: http://localhost:9527");
    axum::serve(listener, app).await.unwrap();
}

async fn get_config(State(state): State<AppState>) -> Json<serde_json::Value> {
    let indices = state.index_list.read().await.clone();
    let stocks = state.stock_list.read().await.clone();
    Json(serde_json::json!({
        "indices": indices,
        "stocks": stocks
    }))
}

async fn get_market_data(State(state): State<AppState>) -> Json<Vec<StockInfo>> {
    Json(state.market_data.read().await.clone())
}

async fn add_stock(
    Query(params): Query<std::collections::HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    if let Some(code) = params.get("code") {
        let mut list = state.stock_list.write().await;
        if !list.contains(code) {
            list.push(code.clone());
            println!("✅ Backend: Added stock {}", code);
            state.data_refresh_notify.notify_one();
            return Json(serde_json::json!({"status": "success", "msg": "added"}));
        }
        state.data_refresh_notify.notify_one();
        return Json(serde_json::json!({"status": "exists", "msg": "already exists"}));
    }
    Json(serde_json::json!({"status": "error", "msg": "no code"}))
}

async fn add_index(
    Query(params): Query<std::collections::HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    if let Some(code) = params.get("code") {
        let mut list = state.index_list.write().await;
        if list.len() >= 8 && !list.contains(code) {
            return Json(serde_json::json!({"status": "full", "msg": "max 8 indices"}));
        }
        if !list.contains(code) {
            list.push(code.clone());
            println!("✅ Backend: Added index {}", code);
            state.data_refresh_notify.notify_one();
            return Json(serde_json::json!({"status": "success", "msg": "added"}));
        }
        state.data_refresh_notify.notify_one();
        return Json(serde_json::json!({"status": "exists", "msg": "already exists"}));
    }
    Json(serde_json::json!({"status": "error", "msg": "no code"}))
}

async fn fetch_realtime_data(state: AppState, notify: Arc<Notify>) {
    // 创建一个复用的 Client，并设置默认 Headers
    let client = reqwest::Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            // 伪装成 Chrome 浏览器
            headers.insert(
                "User-Agent", 
                reqwest::header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            );
            // 设置 Referer，新浪通常会检查这个
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
        
        // 使用带 Header 的 client 发送请求
        match client.get(&url).send().await {
            Ok(resp) => {
                // 检查状态码
                if !resp.status().is_success() {
                    eprintln!("❌ HTTP Error: {}", resp.status());
                    continue;
                }

                match resp.bytes().await {
                    Ok(bytes) => {
                        // GBK 解码
                        let (text, _, _) = GBK.decode(&bytes);
                        
                        // 调试日志
                        println!("📥 Raw data length: {}", text.len());
                        if text.len() < 50 {
                            println!("⚠️ Short response detected: {}", text);
                        } else {
                            println!("📥 Raw data preview: {}", &text[..100]);
                        }
                        
                        let parsed = parse_sina_data(&text);
                        println!("📊 Data refreshed: {} stocks parsed from {} codes", parsed.len(), all_codes.len());
                        
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
        // 只有在数据长度足够但没匹配到时才报错，避免短错误信息刷屏
        if raw_text.len() > 20 {
            eprintln!("⚠️ Regex did not match any data in raw text.");
        }
        return results;
    }

    for cap in RE_SINA_DATA.captures_iter(raw_text) {
        let code = &cap[1];
        let data_str = &cap[2];
        
        let parts: Vec<&str> = data_str.split(',').collect();
        
        // 确保数据完整性
        if parts.len() < 32 { 
            // eprintln!("⚠️ Incomplete data for {}: only {} fields.", code, parts.len());
            continue; 
        }

        let name = parts[0].to_string();
        let open = parts[1].parse::<f64>().unwrap_or(0.0);
        let pre_close = parts[2].parse::<f64>().unwrap_or(0.0);
        let price = parts[3].parse::<f64>().unwrap_or(0.0);
        let high = parts[4].parse::<f64>().unwrap_or(0.0);
        let low = parts[5].parse::<f64>().unwrap_or(0.0);
        let volume = parts[9].parse::<f64>().unwrap_or(0.0); 

        let change = price - pre_close;
        let change_percent = if pre_close > 0.0 { (change / pre_close) * 100.0 } else { 0.0 };

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
        });
    }
    results
}
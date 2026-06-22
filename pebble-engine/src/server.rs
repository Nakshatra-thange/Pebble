use std::sync::{Arc, Mutex};
use std::thread;
use tiny_http::{Server, Response, Header};
use crate::engine::metrics::SharedMetrics;

pub fn start_metrics_server(metrics: SharedMetrics, port: u16) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", port);
        let server = Server::http(&addr).expect("Failed to bind metrics server");
        eprintln!("[HTTP] Metrics server listening on http://localhost:{}/metrics", port);

        for request in server.incoming_requests() {
            let path = request.url().to_string();

            let (status, body, content_type) = if path == "/metrics" || path == "/" {
                let snapshot = metrics.lock().unwrap().snapshot();
                (200, snapshot.to_json(), "application/json")
            } else {
                (404, r#"{"error":"not found"}"#.to_string(), "application/json")
            };

            let cors = Header::from_bytes("Access-Control-Allow-Origin", "*").unwrap();
            let ct = Header::from_bytes("Content-Type", content_type).unwrap();
            let response = Response::from_string(body)
                .with_status_code(status)
                .with_header(cors)
                .with_header(ct);

            let _ = request.respond(response);
        }
    });
}
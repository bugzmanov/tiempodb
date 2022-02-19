use anyhow::{anyhow, Context};
use crossbeam::channel;
use futures::stream::StreamExt;
use hyper::{Body, Server};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tiempodb::sql::query_engine::QueryEngine;
use tiempodb::storage::MetricsData;

pub type Response = hyper::Response<Body>;
pub type Request = hyper::Request<Body>;

struct ServerConfig {
    bind: String,
    storage: StorageConfig,
}

struct StorageConfig {
    data_path: String,
    wal_path: String,
}

enum StorageEvent {
    Ingest(String),
    TimeTick(channel::Sender<()>),
}

const ACK: () = ();

struct TimeTicker {
    outbox: channel::Sender<StorageEvent>,
    ack_receiver: channel::Receiver<()>,
    ack_sender: channel::Sender<()>,
    tick_cycle: Duration,
}

impl TimeTicker {
    pub fn new(outbox: channel::Sender<StorageEvent>, duration: Duration) -> Self {
        let (ack_sender, ack_receiver) = crossbeam::channel::unbounded::<()>(); //todo unbounded
        TimeTicker {
            outbox,
            ack_receiver,
            ack_sender,
            tick_cycle: duration,
        }
    }

    pub fn run(&self) {
        loop {
            std::thread::sleep(self.tick_cycle);
            self.outbox
                .send(StorageEvent::TimeTick(self.ack_sender.clone()))
                .expect("Time tick events reciever is disconnected");
            self.ack_receiver.recv().expect("should not happen"); //todo expect
        }
    }
}

fn main() {
    env_logger::init();
    log::info!("starting tiempodb service");

    let config = ServerConfig {
        bind: "127.0.0.1:8085".into(),
        storage: StorageConfig {
            data_path: "/Users/rafaelbagmanov/workspace/tmp/tiempo/data".into(),
            wal_path: "/Users/rafaelbagmanov/workspace/tmp/tiempo/wal".into(),
        },
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("tiempodb-serve")
        .thread_stack_size(1024 * 1024)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let (sender, receiver) = crossbeam::channel::unbounded::<StorageEvent>(); //todo unbounded

    let time_ticker = TimeTicker::new(sender.clone(), Duration::from_secs(5));

    std::thread::spawn(move || {
        time_ticker.run();
    });

    let storage = tiempodb::storage::SnaphotableStorage::new();
    let snapshot = storage.share_snapshot();

    let mut ingest_engine = tiempodb::ingest::Engine::restore_from_wal(
        storage,
        &std::path::Path::new(&config.storage.wal_path),
        &std::path::Path::new(&config.storage.data_path),
    )
    .expect("storage engine startup");

    std::thread::spawn(move || {
        loop {
            let msg = receiver
                .recv()
                .expect("Can't read data from server, this means that producing service is down");
            match msg {
                StorageEvent::Ingest(data) => match ingest_engine.ingest(&data) {
                    Ok(_r) => {
                        log::debug!("om-nom-nom!")
                        /* do nothing */
                    }
                    Err(e) => {
                        log::error!("failed to ingest infludb line {}", e);
                        todo!("somehow we need to get this back to the user")
                    }
                },
                StorageEvent::TimeTick(sender) => {
                    ingest_engine.time_tick();
                    sender
                        .send(ACK)
                        .expect("Should not happen. Means the sender got killed");
                }
            }
        }
    });

    let tiempo_server = Arc::new(TiempoServer::new(sender, snapshot));
    let service = hyper::service::make_service_fn(move |_conn| {
        let server = tiempo_server.clone();
        async move {
            Ok::<_, std::convert::Infallible>(hyper::service::service_fn(move |request| {
                let server = server.clone();
                async move {
                    match server.tick(request).await {
                        ok @ Ok(_) => ok,
                        Err(x) => Ok(to_http_response(anyhow!(x), 500)),
                    }
                }
            }))
        }
    });

    runtime
        .block_on(async {
            let serve = Server::bind(&(config.bind.parse().expect("hardcoded bind address")))
                .serve(service);
            log::info!("Start serving requests");
            serve.await
        })
        .expect("start service in tokio runtime");
}

async fn body_into_json<T: serde::de::DeserializeOwned>(request: Body) -> anyhow::Result<T> {
    hyper::body::to_bytes(request)
        .await
        .with_context(|| "failed to fetch_body")
        .and_then(|b| {
            String::from_utf8(b.to_vec()).with_context(|| "failed to convert body to utf8 string")
        })
        .and_then(|json| serde_json::from_str::<T>(&json).with_context(|| "failed to parse json"))
}

pub fn to_http_response(err: anyhow::Error, status: u16) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(format!("{{\"error\":\"{:?}\" }}", err).into())
        .expect("mapping from error to Response") // todo: guarantee that it wont fail
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Query {
    query: String,
    #[serde(rename(deserialize = "type", serialize = "type"))]
    query_type: String,
}

struct TiempoServer {
    engine: channel::Sender<StorageEvent>,
    query_engine: QueryEngine,
}

fn parse_query(path_query: Option<&str>) -> Vec<(String, String)> {
    match path_query {
        Some(q_str) => url::form_urlencoded::parse(q_str.as_bytes())
            .into_owned()
            .collect(),
        None => Vec::with_capacity(0),
    }
}

impl TiempoServer {
    fn new(engine: channel::Sender<StorageEvent>, snapshot: Arc<RwLock<MetricsData>>) -> Self {
        TiempoServer {
            engine,
            query_engine: QueryEngine::new(snapshot),
        }
    }

    // todo: multiline json values in case of errors is not OK with the spec
    async fn tick(&self, req: Request) -> Result<Response, String> {
        match *req.method() {
            hyper::Method::POST if req.uri().path().starts_with("/query") => {
                match self.get(req).await {
                    Ok(x) => Ok(x),
                    Err(x) => Ok(x),
                }
            }
            hyper::Method::POST if req.uri().path().starts_with("/write") => self.put(req).await,
            _ => hyper::Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(r#"{"message": "unssuported http method", "error": true}"#.into())
                .map_err(|e| format!("{e}")),
        }
    }

    async fn put(&self, req: Request) -> Result<Response, String> {
        let _query = parse_query(req.uri().query()); //todo: bucket, org, resolution
        let headers = req.headers();
        if let Some(_encoding) = headers.get(hyper::header::CONTENT_ENCODING) {
            //todo: encoding value check
            todo!("gzipped content is not supported yet");
        } else {
            let mut iterator = LinesIterator::new(req.into_body());
            while let Some(next_line) = iterator.next().await {
                let result = match next_line {
                    Ok(line_sr) => self
                        .engine
                        .send(StorageEvent::Ingest(line_sr))
                        .with_context(|| "failed to process incoming lines"), //todo: batching
                    Err(e) => Err(e).with_context(|| "failed to decode incoming lines"),
                };

                if result.is_err() {
                    return hyper::Response::builder()
                        .status(500)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(format!("{{\"error\": \"{:?}\"}}", result).into())
                        .map_err(|e| format!("{e}"));
                }
            }
        }

        hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("ok".into())
            .map_err(|e| format!("{e}"))
    }

    async fn get(&self, req: Request) -> Result<Response, Response> {
        let query = body_into_json::<Query>(req.into_body())
            .await
            .map_err(|e| to_http_response(e, 400))?;
        let result = self
            .query_engine
            .run_query(&query.query)
            .map_err(|e| to_http_response(e, 400))?;

        let json = serde_json::to_string(&result)
            .with_context(|| "failed to parse json")
            .map_err(|e| to_http_response(e, 500))?;
        hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(json.into())
            .with_context(|| "fail to send body")
            .map_err(|e| to_http_response(e, 500))
    }
}

struct LinesIterator {
    body: Body,
    buffer: VecDeque<u8>,
    complete: bool,
}

impl LinesIterator {
    pub fn new(body: Body) -> Self {
        LinesIterator {
            body,
            buffer: VecDeque::with_capacity(1024 * 1024),
            complete: false,
        }
    }

    //todo: this does pretty heavy copying and allocations
    //todo: batching
    pub async fn next(&mut self) -> Option<anyhow::Result<String>> {
        while !self.complete || !self.buffer.is_empty() {
            if let Some((idx, _)) = self.buffer.iter().enumerate().find(|(_, c)| **c == b'\n') {
                let line = String::from_utf8(self.buffer.drain(..idx).collect());
                let _ = self.buffer.drain(..1); //skip '\n' symbol
                return match line {
                    Err(e) => Some(Err(e).with_context(|| "failed to parse utf8 stream")),
                    Ok(line_str) => Some(Ok(line_str)),
                };
            } else if self.complete {
                let line = String::from_utf8(self.buffer.drain(..).collect());
                return match line {
                    Err(e) => Some(Err(e).with_context(|| "failed to parse utf8 stream")),
                    Ok(line_str) => Some(Ok(line_str)),
                };
            }
            if let Some(next) = self.body.next().await {
                match next {
                    Ok(data) => {
                        self.buffer.extend(data.iter());
                    }
                    Err(e) => {
                        return Some(Err(e).with_context(|| "failed to read from http stream"))
                    }
                }
            } else {
                self.complete = true;
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;
    use tiempodb::sql::query_engine::QueryResult;

    #[test]
    fn test_line_terator() {
        let chunks: Vec<Result<_, std::io::Error>> = vec![
            Ok("first_line\nsecond_li"),
            Ok("ne"),
            Ok("\n"),
            Ok("\n"),
            Ok("third_line\n"),
            Ok("fourth_line"),
        ];

        let stream = futures_util::stream::iter(chunks);

        let body = Body::wrap_stream(stream);

        let mut iterator = LinesIterator::new(body);

        let mut result = vec![];
        while let Some(Ok(line)) = tokio_test::block_on(iterator.next()) {
            result.push(line);
        }
        assert_eq!(
            vec!["first_line", "second_line", "", "third_line", "fourth_line"],
            result
        );
    }

    #[test]
    fn test_put() {
        let chunks: Vec<Result<_, std::io::Error>> =
            vec![Ok("first_line\nsecond_line"), Ok("\n"), Ok("third_line")];

        let stream = futures_util::stream::iter(chunks);

        let body = Body::wrap_stream(stream);
        let request = hyper::Request::builder()
            .uri("http://localhost/write?bucket=test_bucket&org=rbag&precision=ms")
            .header("Accept", "application/json")
            .method("POST")
            .body(body)
            .unwrap();

        let (sender, receiver) = crossbeam::channel::unbounded();
        let dumb_snapshot = Arc::new(RwLock::new(HashMap::default()));
        let server = TiempoServer::new(sender, dumb_snapshot);

        let response = dbg!(tokio_test::block_on(server.tick(request)));
        assert_eq!(true, response.is_ok());
        assert_eq!(hyper::StatusCode::OK, response.unwrap().status());

        let v: Vec<String> = receiver
            .try_iter()
            .flat_map(|x| match x {
                StorageEvent::Ingest(line) => Some(line),
                _ => None,
            })
            .collect();
        assert_eq!(v, vec!["first_line", "second_line", "third_line"]);
    }

    #[test]
    fn test_get() {
        let body = serde_json::to_string(&Query {
            query_type: "influxdb".into(),
            query: "SELECT \"name\" FROM \"OLOLO\"".into(),
        })
        .unwrap();
        let request = hyper::Request::builder()
            .uri("http://localhost/query?bucket=test_bucket&org=rbag&precision=ms")
            .header("Accept", "application/json")
            .method("POST")
            .body(body.into())
            .unwrap();

        let (sender, _) = crossbeam::channel::unbounded();
        let dumb_snapshot = Arc::new(RwLock::new(HashMap::default()));
        let server = TiempoServer::new(sender, dumb_snapshot);

        let response = dbg!(tokio_test::block_on(server.tick(request)));
        assert_eq!(true, response.is_ok());
        let response_obj =
            tokio_test::block_on(body_into_json::<QueryResult>(response.unwrap().into_body()))
                .unwrap();
        assert_eq!(
            response_obj.results.get(0).map(|x| x.statement_id.clone()),
            Some("0".into())
        );
    }

    #[test]
    fn test_get_failure_unrecognized_json() {
        let body = r#"{
            "not_": "is what expected"
        }"#;
        let request = hyper::Request::builder()
            .uri("http://localhost/query?bucket=test_bucket&org=rbag&precision=ms")
            .header("Accept", "application/json")
            .method("POST")
            .body(body.into())
            .unwrap();

        let (sender, _) = crossbeam::channel::unbounded();
        let dumb_snapshot = Arc::new(RwLock::new(HashMap::default()));
        let server = TiempoServer::new(sender, dumb_snapshot);
        let response = dbg!(tokio_test::block_on(server.tick(request)));
        assert_eq!(hyper::StatusCode::BAD_REQUEST, response.unwrap().status());
    }

    #[test]
    fn test_get_failute_invalid_json() {
        let body = "{this  is invalid}";
        let request = hyper::Request::builder()
            .uri("http://localhost/query?bucket=test_bucket&org=rbag&precision=ms")
            .header("Accept", "application/json")
            .method("POST")
            .body(body.into())
            .unwrap();
        let (sender, _) = crossbeam::channel::unbounded();
        let dumb_snapshot = Arc::new(RwLock::new(HashMap::default()));
        let server = TiempoServer::new(sender, dumb_snapshot);
        let response = tokio_test::block_on(server.tick(request));
        assert_eq!(hyper::StatusCode::BAD_REQUEST, response.unwrap().status());
    }
}

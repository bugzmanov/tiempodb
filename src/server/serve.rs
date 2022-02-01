use hyper::Body;

pub type Response = hyper::Response<Body>;
pub type Request = hyper::Request<Body>;

struct TiempoServer {}

fn parse_query(path_query: Option<&str>) -> Vec<(String, String)> {
    match path_query {
        Some(q_str) => url::form_urlencoded::parse(q_str.as_bytes())
            .into_owned()
            .collect(),
        None => Vec::with_capacity(0),
    }
}

impl TiempoServer {
    async fn tick(&self, req: Request) -> Result<Response, String> {
        match *req.method() {
            hyper::Method::POST if req.uri().path().starts_with("/query") => self.get(req).await,
            hyper::Method::POST if req.uri().path().starts_with("/write") => self.put(req).await,
            _ => hyper::Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(r#"{"message": "unssuported http method", "error": true}"#.into())
                .map_err(|e| format!("{e}")),
        }
    }

    async fn put(&self, req: Request) -> Result<Response, String> {
        let query = parse_query(req.uri().query());

        hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("ok".into())
            .map_err(|e| format!("{e}"))
    }

    async fn get(&self, req: Request) -> Result<Response, String> {
        hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("ok".into())
            .map_err(|e| format!("{e}"))
    }
}

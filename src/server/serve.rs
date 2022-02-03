use anyhow::Context;
use crossbeam::channel;
use futures::stream::StreamExt;
use futures::AsyncBufReadExt;
use hyper::Body;
use std::collections::VecDeque;

pub type Response = hyper::Response<Body>;
pub type Request = hyper::Request<Body>;

struct TiempoServer {
    engine: channel::Sender<String>,
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
        let query = parse_query(req.uri().query()); //todo: bucket, org, resolution
        let headers = req.headers();
        if let Some(encoding) = headers.get(hyper::header::CONTENT_ENCODING) {
            //todo: encoding value check
            todo!("gzipped content is not supported yet");
        } else {
            let mut iterator = LinesIterator::new(req.into_body());
            while let Some(next_line) = iterator.next().await {
                let result = match next_line {
                    Ok(line_sr) => self
                        .engine
                        .send(line_sr)
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

    async fn get(&self, req: Request) -> Result<Response, String> {
        hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("ok".into())
            .map_err(|e| format!("{e}"))
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

        return None;
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        let server = TiempoServer { engine: sender };

        let response = dbg!(tokio_test::block_on(server.tick(request)));
        assert_eq!(true, response.is_ok());
        assert_eq!(hyper::StatusCode::OK, response.unwrap().status());

        let v: Vec<String> = receiver.try_iter().collect();
        assert_eq!(v, vec!["first_line", "second_line", "third_line"]);
    }
}

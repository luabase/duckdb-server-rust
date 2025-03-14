use axum::{
    http::{Request, HeaderValue},
    response::Response,
};
use tower::{Layer, Service};
use std::{task::{Context, Poll}, future::Future, pin::Pin};

#[derive(Clone)]
pub struct HostnameLayer {
    pub hostname: HeaderValue,
}

impl<S> Layer<S> for HostnameLayer {
    type Service = HostnameMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HostnameMiddleware {
            inner,
            hostname: self.hostname.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HostnameMiddleware<S> {
    inner: S,
    hostname: HeaderValue,
}

impl<S, ReqBody> Service<Request<ReqBody>> for HostnameMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(ctx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let hostname = self.hostname.clone();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut res: Response = fut.await?;
            res.headers_mut().insert("X-Backend-Hostname", hostname);
            Ok(res)
        })
    }
}

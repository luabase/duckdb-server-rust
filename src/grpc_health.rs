use grpc_health_v1::health_check_response::ServingStatus;
pub use grpc_health_v1::health_server::{Health, HealthServer};
pub use grpc_health_v1::{HealthCheckRequest, HealthCheckResponse};
use tonic::{Request, Response, Status};
use tracing::info;

pub mod grpc_health_v1 {
    tonic::include_proto!("grpc.health.v1");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("health_descriptor");
}

#[derive(Default)]
pub struct HealthCheckService {}

#[tonic::async_trait]
impl Health for HealthCheckService {
    async fn check(&self, request: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        let service_name = request.into_inner().service;
        info!("HealthCheck requested for service: {}", service_name);

        Ok(tonic::Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }
}

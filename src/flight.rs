use crate::grpc_health::{grpc_health_v1::FILE_DESCRIPTOR_SET, HealthCheckService, HealthServer};
use crate::{interfaces::QueryParams, state::AppState};
use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer, Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor,
    FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{stream::BoxStream, TryStreamExt};
use std::{net::SocketAddr, sync::Arc};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tonic_reflection::server::Builder as ReflectionBuilder;

pub struct FlightServer {
    pub state: Arc<AppState>,
}

impl FlightServer {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServer {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = request.into_inner().ticket;

        let params: QueryParams = serde_json::from_slice(&ticket_bytes)
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket JSON: {}", e)))?;

        tracing::info!("Flight QueryParams: {:?}", params);

        let db_state = if let Some(dynamic_id) = &params.dynamic_id {
            self.state
                .get_or_create_dynamic_db_state(dynamic_id, &params.database)
                .await
        }
        else {
            self.state.get_or_create_static_db_state(&params.database).await
        }
        .map_err(|e| Status::internal(e.to_string()))?;

        let sql = params
            .sql
            .ok_or_else(|| Status::invalid_argument("SQL query missing"))?;
        let args = params.args.unwrap_or_default();
        let limit = params.limit.unwrap_or(self.state.defaults.row_limit);

        let batches = db_state
            .db
            .get_record_batches(&sql, &args, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let schema = batches
            .first()
            .ok_or_else(|| Status::internal("No batches returned"))?
            .schema();

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>)))
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_put(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        if action.r#type == "healthcheck" {
            let response = arrow_flight::Result {
                body: b"healthy".to_vec().into(),
            };
            let stream = futures::stream::once(async { Ok(response) });
            Ok(Response::new(Box::pin(stream)))
        }
        else {
            Err(Status::unimplemented(format!(
                "Action '{}' not implemented",
                action.r#type
            )))
        }
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![arrow_flight::ActionType {
            r#type: "healthcheck".to_string(),
            description: "Health check action".to_string(),
        }];
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}

pub async fn serve(addr: SocketAddr, state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting Arrow Flight Server at {}", addr);

    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    Server::builder()
        .add_service(FlightServiceServer::new(FlightServer::new(state)))
        .add_service(HealthServer::new(HealthCheckService::default()))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

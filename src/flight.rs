use crate::{interfaces::QueryParams, state::AppState};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
};
use futures::{TryStreamExt, stream::BoxStream};
use std::{net::SocketAddr, sync::Arc};
use tonic::{Request, Response, Status, Streaming, transport::Server};

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

        let db_state = self.state
            .get_or_create_db_state(
                &params.database,
                &params.extensions,
                &params.secrets,
                &params.ducklakes
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let sql = params
            .sql
            .ok_or_else(|| Status::invalid_argument("SQL query is required"))?;

        if sql.trim().is_empty() {
            return Err(Status::invalid_argument("SQL query cannot be empty"));
        }

        let limit = params.limit.unwrap_or(self.state.defaults.row_limit);
        let (query_id, cancel_token) = self.state.start_query(params.database.clone(), sql.clone()).await;

        let result = db_state
            .db
            .get_record_batches(
                &sql,
                &params.args,
                &params.prepare_sql,
                &params.default_schema,
                limit,
                &params.extensions,
                &params.secrets,
                &params.ducklakes,
                &cancel_token
            )
            .await;

        // Always clean up the query from running_queries, regardless of success or failure
        {
            let mut queries = self.state.running_queries.lock().await;
            queries.remove(&query_id);
        }

        let batches = result.map_err(|e| Status::internal(e.to_string()))?;

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

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<FlightServiceServer<FlightServer>>().await;

    Server::builder()
        .add_service(FlightServiceServer::new(FlightServer::new(state)))
        .add_service(health_service)
        .serve(addr)
        .await?;

    Ok(())
}

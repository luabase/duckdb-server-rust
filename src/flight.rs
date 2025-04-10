use crate::{interfaces::QueryParams, state::AppState};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    FlightData, Ticket,
};
use futures::{stream::BoxStream, TryStreamExt};
use std::{net::SocketAddr, sync::Arc};
use tonic::{transport::Server, Request, Response, Status};

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
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type HandshakeStream = BoxStream<'static, Result<arrow_flight::HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;
    type DoPutStream = BoxStream<'static, Result<arrow_flight::PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;

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

        let batches = db_state
            .db
            .get_record_batches(&sql, &args)
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
        _: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_flights(
        &self,
        _: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_flight_info(
        &self,
        _: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_schema(
        &self,
        _: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_put(&self, _: Request<tonic::Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_action(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
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

    async fn list_actions(&self, _: Request<arrow_flight::Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
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

    Server::builder()
        .add_service(FlightServiceServer::new(FlightServer::new(state)))
        .serve(addr)
        .await?;

    Ok(())
}

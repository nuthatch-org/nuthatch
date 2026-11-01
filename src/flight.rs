//! RFC-0079 S2: Arrow Flight SQL beside the HTTP API, behind the off-by-default `flight` feature.
//!
//! A statement is answered by the same router `/sql` is served from: `DoGet` sends it as
//! `POST /<route>/sql` asking for Arrow, so admission, the caps, strict isolation and the memo rules
//! are `/sql`'s own and nothing is implemented twice. The route a client means is named by the
//! [`ROUTE_HEADER`] request header, since a Flight SQL statement carries no catalogue of its own;
//! absent, `/sql` at the root. `GetFlightInfo` runs nothing and holds nothing: its ticket carries the
//! route and the statement, and its schema is empty, the stream's own schema being the answer's.

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status};

/// The request header that names the route a statement is for, as `/<route>/sql` does.
pub const ROUTE_HEADER: &str = "x-nuthatch-route";

/// What a ticket carries: everything `DoGet` needs, so nothing is held between the two calls.
#[derive(serde::Serialize, serde::Deserialize)]
struct Statement {
    route: Option<String>,
    sql: String,
}

#[derive(Clone)]
pub struct Service {
    router: axum::Router,
}

impl Service {
    pub fn new(router: axum::Router) -> Self {
        Self { router }
    }

    /// Serve Flight SQL on `listen` until the process ends.
    pub async fn serve(self, listen: &str) -> anyhow::Result<()> {
        let addr = listen
            .parse()
            .map_err(|e| anyhow::anyhow!("--flight {listen}: {e}"))?;
        tracing::info!("Flight SQL live on {listen}");
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(self))
            .serve(addr)
            .await?;
        Ok(())
    }

    /// `statement` through the router as `/sql` answers it, and the envelope it came with.
    async fn answer(
        &self,
        statement: &Statement,
        authorization: Option<&str>,
    ) -> Result<(bytes::Bytes, Option<String>), Status> {
        use tower::ServiceExt;
        let path = match &statement.route {
            Some(r) => format!("/{}/sql", r.trim_matches('/')),
            None => "/sql".to_string(),
        };
        let mut req = axum::http::Request::post(path)
            .header("content-type", "application/json")
            .header("accept", crate::serve::ARROW_STREAM);
        if let Some(a) = authorization {
            req = req.header("authorization", a);
        }
        let body = serde_json::json!({ "q": statement.sql }).to_string();
        let req = req
            .body(axum::body::Body::from(body))
            .map_err(|e| Status::internal(e.to_string()))?;
        let resp = self
            .router
            .clone()
            .oneshot(req)
            .await
            .unwrap_or_else(|e: std::convert::Infallible| match e {});
        let status = resp.status();
        let envelope = resp
            .headers()
            .get(crate::serve::ANSWER_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !status.is_success() {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            return Err(match status.as_u16() {
                400 | 404 | 422 => Status::invalid_argument(text),
                401 | 403 => Status::permission_denied(text),
                429 | 503 => Status::resource_exhausted(text),
                504 => Status::deadline_exceeded(text),
                _ => Status::internal(text),
            });
        }
        Ok((bytes, envelope))
    }
}

fn header(request_metadata: &tonic::metadata::MetadataMap, name: &str) -> Option<String> {
    request_metadata
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

#[tonic::async_trait]
impl FlightSqlService for Service {
    type FlightService = Service;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let statement = Statement {
            route: header(request.metadata(), ROUTE_HEADER),
            sql: query.query,
        };
        let handle = serde_json::to_vec(&statement).map_err(|e| Status::internal(e.to_string()))?;
        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        Ok(Response::new(
            FlightInfo::new()
                .with_endpoint(endpoint)
                .with_descriptor(request.into_inner())
                .with_total_records(-1),
        ))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let statement: Statement = serde_json::from_slice(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("not a ticket this node issued: {e}")))?;
        let authorization = header(request.metadata(), "authorization");
        let (bytes, envelope) = self.answer(&statement, authorization.as_deref()).await?;
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = reader.schema();
        let batches: Vec<_> = reader
            .collect::<Result<_, _>>()
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from)
            .boxed();
        let mut response = Response::new(stream);
        if let Some(value) = envelope.and_then(|e| e.parse().ok()) {
            response
                .metadata_mut()
                .insert(crate::serve::ANSWER_HEADER, value);
        }
        Ok(response)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

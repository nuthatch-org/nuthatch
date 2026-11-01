//! RFC-0079 S2: Arrow Flight SQL beside the HTTP API, behind the off-by-default `flight` feature.
//!
//! A statement is answered by the same router `/sql` is served from: `DoGet` sends it as
//! `POST /<route>/sql` asking for Arrow, so admission, the caps, strict isolation and the memo rules
//! are `/sql`'s own and nothing is implemented twice. The route a client means is named by the
//! [`ROUTE_HEADER`] request header, since a Flight SQL statement carries no catalogue of its own;
//! absent, `/sql` at the root. `GetFlightInfo` runs nothing and holds nothing: its ticket carries the
//! route and the statement, and its schema is empty, the stream's own schema being the answer's.
//!
//! The catalogue calls (`GetCatalogs`, `GetDbSchemas`, `GetTables`, `GetTableTypes`) are statements
//! too: `information_schema.tables` through the same door, and a table's schema is its own
//! `SELECT * … LIMIT 0`, so they list exactly what `/sql` on that route would answer.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
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

/// One relation `/sql` answers on a route, as `information_schema.tables` names it.
struct Relation {
    catalog: String,
    schema: String,
    name: String,
    kind: String,
}

type DoGetStream = <Service as FlightService>::DoGetStream;

#[derive(Clone)]
pub struct Service {
    router: axum::Router,
    sql_info: std::sync::Arc<SqlInfoData>,
}

impl Service {
    pub fn new(router: axum::Router) -> Self {
        let mut info = SqlInfoDataBuilder::new();
        info.append(SqlInfo::FlightSqlServerName, "nuthatch");
        info.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        info.append(SqlInfo::FlightSqlServerArrowVersion, arrow::ARROW_VERSION);
        info.append(SqlInfo::FlightSqlServerReadOnly, true);
        info.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        let sql_info = std::sync::Arc::new(info.build().expect("a fixed SqlInfo table builds"));
        Self { router, sql_info }
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
            return Err(refusal(status.as_u16(), &bytes));
        }
        Ok((bytes, envelope))
    }

    /// The answer to `statement` as Arrow batches.
    async fn batches(
        &self,
        statement: &Statement,
        authorization: Option<&str>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, Option<String>), Status> {
        let (bytes, envelope) = self.answer(statement, authorization).await?;
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<_, _>>()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok((schema, batches, envelope))
    }

    /// The relations `/sql` answers on the route `request` names.
    async fn relations<T>(&self, request: &Request<T>) -> Result<Vec<Relation>, Status> {
        let statement = Statement {
            route: header(request.metadata(), ROUTE_HEADER),
            sql: "SELECT table_catalog, table_schema, table_name, table_type \
                  FROM information_schema.tables"
                .into(),
        };
        let auth = header(request.metadata(), "authorization");
        let (_, batches, _) = self.batches(&statement, auth.as_deref()).await?;
        let mut out = Vec::new();
        for batch in &batches {
            let cols: Vec<_> = (0..4)
                .map(|i| arrow::compute::cast(batch.column(i), &DataType::Utf8))
                .collect::<Result<_, _>>()
                .map_err(|e| Status::internal(e.to_string()))?;
            let cols: Vec<_> = cols.iter().map(|c| c.as_string::<i32>()).collect();
            for row in 0..batch.num_rows() {
                let at = |c: usize| {
                    cols[c]
                        .is_valid(row)
                        .then(|| cols[c].value(row).to_string())
                };
                let (Some(catalog), Some(schema), Some(name), Some(kind)) =
                    (at(0), at(1), at(2), at(3))
                else {
                    continue;
                };
                out.push(Relation {
                    catalog,
                    schema,
                    name,
                    kind,
                });
            }
        }
        out.sort_by(|a, b| (&a.catalog, &a.schema, &a.name).cmp(&(&b.catalog, &b.schema, &b.name)));
        Ok(out)
    }

    /// A relation's Arrow schema, as a statement over it would stream it.
    async fn relation_schema<T>(
        &self,
        request: &Request<T>,
        relation: &Relation,
    ) -> Result<SchemaRef, Status> {
        let statement = Statement {
            route: header(request.metadata(), ROUTE_HEADER),
            sql: format!(
                "SELECT * FROM \"{}\" LIMIT 0",
                relation.name.replace('"', "\"\"")
            ),
        };
        let auth = header(request.metadata(), "authorization");
        Ok(self.batches(&statement, auth.as_deref()).await?.0)
    }
}

/// `/sql`'s refusal as the gRPC status a Flight client expects, carrying `/sql`'s JSON error.
fn refusal(http: u16, body: &[u8]) -> Status {
    let text = String::from_utf8_lossy(body).into_owned();
    match http {
        400 | 422 => Status::invalid_argument(text),
        401 => Status::unauthenticated(text),
        403 => Status::permission_denied(text),
        404 => Status::not_found(text),
        429 | 503 => Status::resource_exhausted(text),
        504 => Status::deadline_exceeded(text),
        _ => Status::internal(text),
    }
}

fn header(request_metadata: &tonic::metadata::MetadataMap, name: &str) -> Option<String> {
    request_metadata
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// SQL `LIKE` as Flight SQL's filter patterns use it: `%` any run, `_` one character, no escape.
fn like(pattern: &str, s: &str) -> bool {
    let (p, s): (Vec<char>, Vec<char>) = (pattern.chars().collect(), s.chars().collect());
    let (mut pi, mut si, mut star, mut mark) = (0, 0, None, 0);
    while si < s.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some(pi);
            mark = si;
            pi += 1;
        } else if let Some(at) = star {
            pi = at + 1;
            mark += 1;
            si = mark;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '%')
}

/// A `FlightInfo` whose one ticket is the command itself, for the catalogue calls.
fn command_info(
    command: impl ProstMessageExt,
    schema: &arrow::datatypes::Schema,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: command.as_any().encode_to_vec().into(),
    });
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(e.to_string()))?
        .with_endpoint(endpoint)
        .with_descriptor(descriptor)
        .with_total_records(-1);
    Ok(Response::new(info))
}

fn stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> DoGetStream {
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(batches.into_iter().map(Ok)))
        .map_err(Status::from)
        .boxed()
}

fn one(
    batch: Result<RecordBatch, arrow_flight::error::FlightError>,
) -> Result<Response<DoGetStream>, Status> {
    let batch = batch.map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(stream(batch.schema(), vec![batch])))
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
    ) -> Result<Response<DoGetStream>, Status> {
        let statement: Statement = serde_json::from_slice(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("not a ticket this node issued: {e}")))?;
        let authorization = header(request.metadata(), "authorization");
        let (schema, batches, envelope) =
            self.batches(&statement, authorization.as_deref()).await?;
        let mut response = Response::new(stream(schema, batches));
        if let Some(value) = envelope.and_then(|e| e.parse().ok()) {
            response
                .metadata_mut()
                .insert(crate::serve::ANSWER_HEADER, value);
        }
        Ok(response)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        command_info(query, &schema, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut catalogs: Vec<String> = self
            .relations(&request)
            .await?
            .into_iter()
            .map(|r| r.catalog)
            .collect();
        catalogs.dedup();
        let mut builder = query.into_builder();
        for c in catalogs {
            builder.append(c);
        }
        one(builder.build())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        command_info(query, &schema, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut pairs: Vec<(String, String)> = self
            .relations(&request)
            .await?
            .into_iter()
            .map(|r| (r.catalog, r.schema))
            .collect();
        pairs.dedup();
        let mut builder = query.into_builder();
        for (catalog, schema) in pairs {
            builder.append(catalog, schema);
        }
        one(builder.build())
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        command_info(query, &schema, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let empty = arrow::datatypes::Schema::empty();
        let relations = self.relations(&request).await?;
        let wanted = |r: &Relation| {
            query.catalog.as_ref().is_none_or(|c| *c == r.catalog)
                && query
                    .db_schema_filter_pattern
                    .as_ref()
                    .is_none_or(|p| like(p, &r.schema))
                && query
                    .table_name_filter_pattern
                    .as_ref()
                    .is_none_or(|p| like(p, &r.name))
                && (query.table_types.is_empty() || query.table_types.contains(&r.kind))
        };
        let include_schema = query.include_schema;
        let mut builder = query.clone().into_builder();
        // Filtered here as well as by the builder, so a schema is asked for only where it is listed.
        for r in relations.iter().filter(|r| wanted(r)) {
            let schema = if include_schema {
                Some(self.relation_schema(&request, r).await?)
            } else {
                None
            };
            builder
                .append(
                    &r.catalog,
                    &r.schema,
                    &r.name,
                    &r.kind,
                    schema.as_deref().unwrap_or(&empty),
                )
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        one(builder.build())
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        command_info(query, &schema, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut kinds: Vec<String> = self
            .relations(&request)
            .await?
            .into_iter()
            .map(|r| r.kind)
            .collect();
        kinds.sort();
        kinds.dedup();
        let mut builder = query.into_builder();
        for k in kinds {
            builder.append(k);
        }
        one(builder.build())
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(&self.sql_info).schema();
        command_info(query, &schema, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        one(query.into_builder(&self.sql_info).build())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::like;

    #[test]
    fn like_matches_as_flight_sql_filter_patterns_do() {
        for (p, s, want) in [
            ("%", "", true),
            ("%", "tab__transfer", true),
            ("tab__transfer", "tab__transfer", true),
            ("tab%", "tab__transfer", true),
            ("%transfer", "tab__transfer", true),
            ("t_b%", "tab__transfer", true),
            ("t_b", "tab__transfer", false),
            ("%x%", "tab__transfer", false),
            ("which_nest", "whichxnest", true),
            ("which_nest", "which_nests", false),
            ("%a%a%", "banana", true),
        ] {
            assert_eq!(like(p, s), want, "{p:?} LIKE {s:?}");
        }
    }

    /// The caller's `authorization` reaches the router, and the router's `401` reaches the caller as
    /// `Unauthenticated` with its JSON. Nothing in nuthatch checks a credential on `/sql`; a layer an
    /// operator puts in front of it does, which is what this router stands in for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn authorization_is_passed_through_and_its_refusal_comes_back() {
        use arrow_flight::sql::client::FlightSqlServiceClient;
        use std::sync::Arc;

        async fn sql(headers: axum::http::HeaderMap) -> axum::response::Response {
            use axum::response::IntoResponse;
            let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
                let body = axum::Json(serde_json::json!({ "error": "no credential" }));
                return (axum::http::StatusCode::UNAUTHORIZED, body).into_response();
            };
            let batch = arrow::array::RecordBatch::try_from_iter([(
                "seen",
                Arc::new(arrow::array::StringArray::from(vec![auth])) as arrow::array::ArrayRef,
            )])
            .unwrap();
            let mut ipc = Vec::new();
            let mut w =
                arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            drop(w);
            ipc.into_response()
        }
        let router = axum::Router::new().route("/m/sql", axum::routing::post(sql));
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let service = super::Service::new(router);
        tokio::spawn(async move { service.serve(&addr.to_string()).await });
        let channel = loop {
            match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
            {
                Ok(c) => break c,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        };
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header(super::ROUTE_HEADER, "m");
        let ask = async |client: &mut FlightSqlServiceClient<_>| {
            let info = client.execute("SELECT 1".into(), None).await?;
            let ticket = info.endpoint[0].ticket.clone().unwrap();
            futures::TryStreamExt::try_collect::<Vec<_>>(client.do_get(ticket).await?).await
        };

        let refused = ask(&mut client).await.unwrap_err();
        let status = tonic::Status::from(refused);
        assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status}");
        assert_eq!(status.message(), r#"{"error":"no credential"}"#);

        client.set_token("s3cret".into());
        let batches = ask(&mut client).await.unwrap();
        let seen = arrow::array::AsArray::as_string::<i32>(batches[0].column(0)).value(0);
        assert_eq!(seen, "Bearer s3cret");
    }
}

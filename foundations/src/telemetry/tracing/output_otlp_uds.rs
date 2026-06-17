//! [OTLP-over-UDS] output for the user tracing pipeline.
//!
//! Sends protobuf-encoded OTLP trace data over HTTP/1.1 to a Unix domain
//! socket served by a local OTLP receptor.
//
// The export logic is intentionally split so this client mirrors the shape of a
// `BatchHandler`: `OtlpUdsClient::process_batch` is the per-batch unit (the
// future trait method), while the `do_export` drain loop stays generic. This
// keeps the door open to later relocating the client into oxy as a plugin
// without reshaping the per-batch logic.

use super::channel::SharedSpanReceiver;
use super::init::TraceOutputFutures;
use super::internal::reporter_error;
use crate::telemetry::otlp_conversion::tracing::convert_span;
use crate::telemetry::settings::OtlpUdsOutputSettings;
use crate::{BootstrapResult, ServiceInfo};
use anyhow::ensure;
use cf_rustracing::span::RoutingMetadata;
use cf_rustracing_jaeger::span::FinishedSpan;
use futures_util::future::FutureExt as _;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message as _;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UnixStream;

const TRACES_PATH: &str = "/v1/traces";
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
const TRACE_CONFIG_HEADER: &str = "cf-trace-config";
const HOST_HEADER_VALUE: &str = "localhost";

/// A failure exporting a single OTLP request over the Unix domain socket.
///
/// This is the concrete error type for the UDS client, mirroring how the
/// existing exporters surface a concrete library error (`cf_rustracing::Error`,
/// `tonic::Status`) to [`reporter_error`].
#[derive(Debug)]
enum OtlpUdsExportError {
    Connect(std::io::Error),
    Handshake(hyper::Error),
    BuildRequest(http::Error),
    Send(hyper::Error),
    Status(StatusCode),
}

impl std::fmt::Display for OtlpUdsExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(err) => write!(f, "failed to connect to UDS socket: {err}"),
            Self::Handshake(err) => write!(f, "HTTP/1 handshake over UDS failed: {err}"),
            Self::BuildRequest(err) => write!(f, "failed to build OTLP UDS request: {err}"),
            Self::Send(err) => write!(f, "failed to send OTLP UDS request: {err}"),
            Self::Status(status) => {
                write!(f, "OTLP UDS receptor returned non-success status: {status}")
            }
        }
    }
}

impl std::error::Error for OtlpUdsExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::Handshake(err) => Some(err),
            Self::BuildRequest(err) => Some(err),
            Self::Send(err) => Some(err),
            Self::Status(_) => None,
        }
    }
}

/// Encodes a span's [`RoutingMetadata`] into the `cf-trace-config` header value.
///
/// This is foundations' contract with the receptor, so the wire shape lives here
/// (not on the `RoutingMetadata` type) and can evolve independently. It's a
/// simple JSON object of the routing fields.
fn encode_trace_config(routing: &RoutingMetadata) -> String {
    serde_json::json!({
        "zoneId": routing.zone_id,
        "accountId": routing.account_id,
        "workspaceId": routing.workspace_id,
        "destinations": routing.destinations,
        "managed": routing.managed,
    })
    .to_string()
}

/// Exports user tracing spans as OTLP over a Unix domain socket.
#[derive(Debug)]
pub(super) struct OtlpUdsClient {
    socket_path: String,
}

impl OtlpUdsClient {
    pub(super) fn new(settings: &OtlpUdsOutputSettings) -> BootstrapResult<Self> {
        ensure!(
            !settings.socket_path.is_empty(),
            "user tracing OTLP UDS `socket_path` must be set"
        );

        Ok(Self {
            socket_path: settings.socket_path.clone(),
        })
    }

    /// Processes a single drained batch of spans: groups them by zone, converts
    /// each to OTLP, and POSTs one request per zone. Errors are reported and do
    /// not abort the batch.
    ///
    /// This is shaped like a `BatchHandler::process_batch` so the client can be
    /// lifted into a plugin later without reworking the per-batch logic.
    async fn process_batch(&self, service_info: &ServiceInfo, spans: Vec<FinishedSpan>) {
        // Group spans by zone so each request carries a single zone's routing
        // metadata in its `cf-trace-config` header.
        let mut groups: HashMap<u64, (RoutingMetadata, Vec<ResourceSpans>)> = HashMap::new();

        for span in spans {
            // Spans without routing metadata aren't user-traced spans we can
            // route, so drop them. Read routing before `convert_span` consumes
            // the span.
            let Some(routing) = span.routing().cloned() else {
                continue;
            };
            let zone_id = routing.zone_id;
            let resource_spans = convert_span(span, service_info);

            groups
                .entry(zone_id)
                .or_insert_with(|| (routing, Vec::new()))
                .1
                .push(resource_spans);
        }

        for (_zone_id, (routing, resource_spans)) in groups {
            let body = ExportTraceServiceRequest { resource_spans }.encode_to_vec();
            let trace_config = encode_trace_config(&routing);

            if let Err(err) = self.send(body, trace_config).await {
                reporter_error(err);
            }
        }
    }

    /// POSTs a single OTLP request body to the receptor, tagged with the
    /// per-zone `cf-trace-config` header.
    async fn send(&self, body: Vec<u8>, trace_config: String) -> Result<(), OtlpUdsExportError> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(OtlpUdsExportError::Connect)?;

        let (mut send_request, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(OtlpUdsExportError::Handshake)?;

        // Drive the connection in the background; request/response errors are
        // surfaced via `send_request` below, and the driver completes once the
        // exchange is done.
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri(TRACES_PATH)
            .header(HOST, HOST_HEADER_VALUE)
            .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
            .header(TRACE_CONFIG_HEADER, trace_config)
            .body(Full::new(Bytes::from(body)))
            .map_err(OtlpUdsExportError::BuildRequest)?;

        let response = send_request
            .send_request(request)
            .await
            .map_err(OtlpUdsExportError::Send)?;

        let status = response.status();
        if !status.is_success() {
            return Err(OtlpUdsExportError::Status(status));
        }

        Ok(())
    }
}

pub(super) fn start(
    service_info: &ServiceInfo,
    settings: &OtlpUdsOutputSettings,
    span_rx: SharedSpanReceiver,
) -> BootstrapResult<TraceOutputFutures> {
    let client = Arc::new(OtlpUdsClient::new(settings)?);
    let max_batch_size = settings.max_batch_size;

    let workers = (0..settings.num_tasks)
        .map(|_| {
            let client = Arc::clone(&client);
            let service_info = service_info.clone();
            let span_rx = span_rx.clone();

            async move { do_export(client, service_info, span_rx, max_batch_size).await }.boxed()
        })
        .collect();

    Ok(TraceOutputFutures {
        initializer: None,
        workers,
    })
}

/// Drains the span channel and hands each batch to the client. This loop is the
/// generic part (the future `run_export_loop`); the client owns the per-batch
/// work.
async fn do_export(
    client: Arc<OtlpUdsClient>,
    service_info: ServiceInfo,
    span_rx: SharedSpanReceiver,
    max_batch_size: usize,
) {
    let mut batch = Vec::with_capacity(max_batch_size);

    while span_rx.recv_many(&mut batch, max_batch_size).await > 0 {
        client
            .process_batch(&service_info, std::mem::take(&mut batch))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt as _;
    use hyper::Response;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use std::convert::Infallible;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tokio::sync::mpsc;

    struct CapturedRequest {
        method: String,
        path: String,
        host: Option<String>,
        content_type: Option<String>,
        trace_config: Option<String>,
        body: Vec<u8>,
    }

    /// Binds a UDS "receptor" that captures the first request it receives and
    /// replies with `status`. The returned `TempDir` must be kept alive for the
    /// socket file to remain valid.
    fn spawn_receptor(
        status: StatusCode,
    ) -> (PathBuf, TempDir, mpsc::UnboundedReceiver<CapturedRequest>) {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("otlp.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();

        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            let service = service_fn(move |req: Request<Incoming>| {
                let tx = tx.clone();

                async move {
                    let (parts, body) = req.into_parts();
                    let headers = &parts.headers;
                    let get = |name: &str| {
                        headers
                            .get(name)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned)
                    };

                    let captured = CapturedRequest {
                        method: parts.method.to_string(),
                        path: parts.uri.path().to_string(),
                        host: get("host"),
                        content_type: get("content-type"),
                        trace_config: get(TRACE_CONFIG_HEADER),
                        body: body.collect().await.unwrap().to_bytes().to_vec(),
                    };

                    tx.send(captured).ok();

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                }
            });

            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
                .ok();
        });

        (socket_path, dir, rx)
    }

    fn settings_for(socket_path: &Path) -> OtlpUdsOutputSettings {
        OtlpUdsOutputSettings {
            socket_path: socket_path.to_string_lossy().into_owned(),
            num_tasks: 1,
            max_batch_size: 8,
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_socket_path() {
        let err = OtlpUdsClient::new(&OtlpUdsOutputSettings {
            socket_path: String::new(),
            num_tasks: 1,
            max_batch_size: 8,
        })
        .unwrap_err();

        assert!(err.to_string().contains("socket_path"));
    }

    #[tokio::test]
    async fn send_posts_otlp_with_headers_and_body() {
        let (socket_path, _dir, mut rx) = spawn_receptor(StatusCode::OK);

        let client = OtlpUdsClient::new(&settings_for(&socket_path)).unwrap();

        let body = b"hello-otlp".to_vec();
        let trace_config = r#"{"zone_id":"z1"}"#.to_string();

        client
            .send(body.clone(), trace_config.clone())
            .await
            .unwrap();

        let captured = rx.recv().await.unwrap();
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path, TRACES_PATH);
        assert_eq!(captured.host.as_deref(), Some(HOST_HEADER_VALUE));
        assert_eq!(
            captured.content_type.as_deref(),
            Some(CONTENT_TYPE_PROTOBUF)
        );
        assert_eq!(
            captured.trace_config.as_deref(),
            Some(trace_config.as_str())
        );
        assert_eq!(captured.body, body);
    }

    #[tokio::test]
    async fn send_errors_on_non_success_status() {
        let (socket_path, _dir, _rx) = spawn_receptor(StatusCode::INTERNAL_SERVER_ERROR);

        let client = OtlpUdsClient::new(&settings_for(&socket_path)).unwrap();

        let err = client
            .send(b"x".to_vec(), "{}".to_string())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            OtlpUdsExportError::Status(StatusCode::INTERNAL_SERVER_ERROR)
        ));
        assert!(err.to_string().contains("non-success status"));
    }

    // Drives the full path: a span produced through a tracer (with routing set)
    // flows through the channel, is converted + encoded by `process_batch`, and
    // arrives at the receptor with its routing in the `cf-trace-config` header.
    #[tokio::test]
    async fn process_batch_sends_converted_spans() {
        use super::super::channel::unbounded_channel;
        use cf_rustracing::Tracer;
        use cf_rustracing::sampler::AllSampler;

        let (socket_path, _dir, mut rx) = spawn_receptor(StatusCode::OK);

        let (sender, span_rx) = unbounded_channel();

        // Produce one finished span with routing, then drop the tracer so the
        // channel closes and the worker loop terminates after draining.
        {
            let tracer = Tracer::with_consumer(AllSampler, sender);
            let mut span = tracer.span("user-root").start();
            span.set_routing(RoutingMetadata {
                zone_id: 12345,
                account_id: 42,
                workspace_id: "ws-1".to_string(),
                destinations: vec!["dest-a".to_string()],
                managed: true,
            });
        }

        let service_info = crate::service_info!();
        let futs = start(&service_info, &settings_for(&socket_path), span_rx).unwrap();
        for worker in futs.workers {
            tokio::spawn(worker);
        }

        let captured = rx.recv().await.unwrap();
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path, TRACES_PATH);
        assert_eq!(
            captured.content_type.as_deref(),
            Some(CONTENT_TYPE_PROTOBUF)
        );
        let trace_config: serde_json::Value =
            serde_json::from_str(captured.trace_config.as_deref().unwrap()).unwrap();
        assert_eq!(trace_config["zoneId"], 12345);
        assert_eq!(trace_config["accountId"], 42);
        assert_eq!(trace_config["workspaceId"], "ws-1");
        assert_eq!(trace_config["destinations"], serde_json::json!(["dest-a"]));
        assert_eq!(trace_config["managed"], true);
        // Body is a protobuf-encoded `ExportTraceServiceRequest`.
        assert!(!captured.body.is_empty());
    }
}

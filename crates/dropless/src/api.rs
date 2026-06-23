//! The axum HTTP API.
//!
//! Auth: `Authorization: Bearer <key>` on every `/v1/*` route. The key is
//! sha256-hashed and looked up in `api_keys`; the owning tenant is attached to
//! the request. `/`, `/healthz`, `/readyz`, and `/metrics` are public.

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use std::sync::Arc;

use dropless_core::inbound::{self, Provider};
use dropless_core::inbox::{self, Accepted};
use dropless_core::notify::Notifier;
use dropless_core::{outbox, store, CoreError};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// Max inbound webhook body. Provider payloads are small JSON; cap well under
/// axum's 2MB default. (Global rate-limiting / slow-loris / concurrency limits
/// for the public `/ingest` route are expected from a fronting reverse proxy.)
const INGEST_BODY_LIMIT: usize = 256 * 1024;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    /// Database pool (source of truth + queue).
    pub pool: PgPool,
    /// Coalesced waker that NOTIFYs idle dispatch workers after an enqueue.
    pub notifier: Arc<Notifier>,
    /// If set, `/metrics` requires `Authorization: Bearer <token>`. When unset,
    /// `/metrics` is open (so it must be firewalled in multi-tenant deploys).
    pub metrics_token: Option<Arc<str>>,
    /// Operator token for the cross-tenant `/admin` console. `None` = disabled.
    pub admin_token: Option<Arc<str>>,
    /// Per-tenant ingest rate limiter (a no-op when disabled).
    pub limiter: Arc<crate::ratelimit::RateLimiter>,
}

/// The authenticated tenant, attached by the auth middleware.
#[derive(Clone)]
struct Tenant(String);

/// Build the full router (public + authenticated routes).
pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/messages", post(create_message).get(list_messages))
        .route("/v1/messages/{id}", get(get_message))
        .route("/v1/deliveries/{id}", get(get_delivery_detail))
        .route("/v1/deliveries/{id}/replay", post(replay_delivery))
        .route(
            "/v1/endpoints",
            post(create_endpoint_handler).get(list_endpoints_handler),
        )
        .route(
            "/v1/endpoints/{id}",
            get(get_endpoint_handler).patch(update_endpoint_handler),
        )
        // Consumer (application) tier: a tenant's own end-customers. A message
        // published to a consumer fans out only to that consumer's endpoints.
        .route(
            "/v1/app",
            post(create_consumer_handler).get(list_consumers_handler),
        )
        .route("/v1/app/{uid}", get(get_consumer_handler))
        .route(
            "/v1/app/{uid}/endpoints",
            post(create_consumer_endpoint_handler).get(list_consumer_endpoints_handler),
        )
        .route(
            "/v1/app/{uid}/messages",
            post(create_consumer_message_handler).get(list_consumer_messages_handler),
        )
        .route(
            "/v1/inbound-sources",
            post(create_inbound_source_handler).get(list_inbound_sources_handler),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), auth));

    Router::new()
        .route("/", get(dashboard))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        // Inbound gateway: providers authenticate by signature, not API key.
        .route(
            "/ingest/{slug}",
            post(ingest).layer(DefaultBodyLimit::max(INGEST_BODY_LIMIT)),
        )
        // Operator admin console (cross-tenant); gated by ADMIN_TOKEN.
        .route("/admin", get(admin_dashboard))
        .route("/admin/overview", get(admin_overview))
        .route("/admin/failures", get(admin_failures))
        .merge(protected)
        .with_state(state)
}

/// Bearer-token authentication middleware.
async fn auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    let Some(token) = token else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let key_hash = hash_key(token);
    match store::tenant_for_api_key(&state.pool, &key_hash).await {
        Ok(Some(tenant)) => {
            req.extensions_mut().insert(Tenant(tenant));
            Ok(next.run(req).await)
        }
        Ok(None) => Err(StatusCode::UNAUTHORIZED),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// sha256 hex of an API key (matches what is stored in `api_keys.key_hash`).
pub fn hash_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

// ---- handlers ---------------------------------------------------------------

/// `POST /v1/messages` — create an event and fan it out. Returns `201` only
/// after the event and its deliveries are committed (the core invariant).
#[derive(Deserialize)]
struct CreateMessage {
    event_type: String,
    payload: serde_json::Value,
}

#[derive(Serialize)]
struct CreateMessageResponse {
    id: Uuid,
    delivery_ids: Vec<Uuid>,
}

async fn create_message(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(body): Json<CreateMessage>,
) -> Response {
    if let Some(retry_after) = state.limiter.check(&tenant) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", format!("{retry_after:.0}"))],
            Json(serde_json::json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
    }
    match outbox::enqueue_message(&state.pool, &tenant, None, &body.event_type, &body.payload).await
    {
        Ok(enq) => {
            // Wake idle workers immediately (coalesced, off the response path).
            state.notifier.ping();
            (
                StatusCode::CREATED,
                Json(CreateMessageResponse {
                    id: enq.event_id,
                    delivery_ids: enq.delivery_ids,
                }),
            )
                .into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// `GET /v1/messages` — recent events for the tenant (newest first).
#[derive(Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_messages(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<dropless_core::model::Event>>, ApiError> {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let offset = q.offset.unwrap_or(0).max(0);
    Ok(Json(
        store::list_events(&state.pool, &tenant, limit, offset).await?,
    ))
}

/// `GET /v1/messages/{id}` — an event and its deliveries.
#[derive(Serialize)]
struct MessageResponse {
    event: dropless_core::model::Event,
    deliveries: Vec<dropless_core::model::Delivery>,
}

async fn get_message(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<MessageResponse>, ApiError> {
    let event = store::get_event(&state.pool, id)
        .await?
        .ok_or_else(|| CoreError::NotFound(format!("event {id}")))?;
    // Tenant isolation: never expose another tenant's event.
    if event.tenant_id != tenant {
        return Err(CoreError::NotFound(format!("event {id}")).into());
    }
    let deliveries = store::deliveries_for_event(&state.pool, id).await?;
    Ok(Json(MessageResponse { event, deliveries }))
}

/// `GET /v1/deliveries/{id}` — a delivery and its full attempt timeline.
#[derive(Serialize)]
struct DeliveryDetail {
    delivery: dropless_core::model::Delivery,
    attempts: Vec<dropless_core::model::DeliveryAttempt>,
}

async fn get_delivery_detail(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<DeliveryDetail>, ApiError> {
    let delivery = store::get_delivery(&state.pool, id)
        .await?
        .filter(|d| d.tenant_id == tenant)
        .ok_or_else(|| CoreError::NotFound(format!("delivery {id}")))?;
    let attempts = store::attempts_for_delivery(&state.pool, id).await?;
    Ok(Json(DeliveryDetail { delivery, attempts }))
}

/// `POST /v1/deliveries/{id}/replay` — re-queue a delivery immediately.
async fn replay_delivery(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let delivery = store::require_delivery(&state.pool, id).await?;
    if delivery.tenant_id != tenant {
        return Err(CoreError::NotFound(format!("delivery {id}")).into());
    }
    store::reset_for_replay(&state.pool, id).await?;
    Ok(StatusCode::ACCEPTED)
}

// ---- endpoint management ----------------------------------------------------

#[derive(Deserialize)]
struct CreateEndpoint {
    url: String,
    /// Optional signing secret; a Svix-style `whsec_…` secret is generated if omitted.
    secret: Option<String>,
    /// Event types to deliver (exact, `prefix.*`, or `*`). Omit/null = ALL events.
    event_types: Option<Vec<String>>,
}

#[derive(Serialize)]
struct CreatedEndpoint {
    #[serde(flatten)]
    endpoint: dropless_core::model::Endpoint,
    /// Returned ONLY on creation — store it now; it is never exposed again.
    secret: String,
}

#[derive(Deserialize)]
struct UpdateEndpoint {
    url: Option<String>,
    disabled: Option<bool>,
    /// Replace the event-type subscriptions (omit to leave unchanged).
    event_types: Option<Vec<String>>,
}

/// `POST /v1/endpoints` — register a subscriber endpoint for the tenant.
async fn create_endpoint_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(body): Json<CreateEndpoint>,
) -> Result<(StatusCode, Json<CreatedEndpoint>), ApiError> {
    let secret = body
        .secret
        .unwrap_or_else(dropless_core::signing::generate_secret);
    let endpoint = store::create_endpoint(
        &state.pool,
        &tenant,
        None,
        &body.url,
        &secret,
        body.event_types.as_deref(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedEndpoint { endpoint, secret }),
    ))
}

/// `GET /v1/endpoints` — list the tenant's endpoints (secrets are never shown).
async fn list_endpoints_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
) -> Result<Json<Vec<dropless_core::model::Endpoint>>, ApiError> {
    Ok(Json(store::list_endpoints(&state.pool, &tenant).await?))
}

/// `GET /v1/endpoints/{id}` — one endpoint, tenant-scoped.
async fn get_endpoint_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<dropless_core::model::Endpoint>, ApiError> {
    let ep = store::get_endpoint(&state.pool, id)
        .await?
        .filter(|e| e.tenant_id == tenant)
        .ok_or_else(|| CoreError::NotFound(format!("endpoint {id}")))?;
    Ok(Json(ep))
}

/// `PATCH /v1/endpoints/{id}` — update URL and/or the disabled flag.
async fn update_endpoint_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateEndpoint>,
) -> Result<Json<dropless_core::model::Endpoint>, ApiError> {
    let ep = store::update_endpoint(
        &state.pool,
        id,
        &tenant,
        body.url.as_deref(),
        body.disabled,
        body.event_types.as_deref(),
    )
    .await?
    .ok_or_else(|| CoreError::NotFound(format!("endpoint {id}")))?;
    Ok(Json(ep))
}

// ---- consumers (the application tier) ---------------------------------------

/// Resolve a tenant's consumer by uid, or 404. Tenant-scoped: a tenant can only
/// reach its own consumers.
async fn resolve_consumer(
    state: &AppState,
    tenant: &str,
    uid: &str,
) -> Result<dropless_core::model::Consumer, ApiError> {
    store::get_consumer(&state.pool, tenant, uid)
        .await?
        .ok_or_else(|| CoreError::NotFound(format!("consumer {uid}")).into())
}

#[derive(Deserialize)]
struct CreateConsumer {
    /// SaaS-assigned customer id, unique within the tenant.
    uid: String,
    /// Optional human-readable label.
    name: Option<String>,
}

/// `POST /v1/app` — get-or-create a consumer (idempotent on `uid`).
async fn create_consumer_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(body): Json<CreateConsumer>,
) -> Result<(StatusCode, Json<dropless_core::model::Consumer>), ApiError> {
    if body.uid.trim().is_empty() {
        return Err(CoreError::Invalid("uid must not be empty".into()).into());
    }
    let consumer =
        store::create_consumer(&state.pool, &tenant, &body.uid, body.name.as_deref()).await?;
    Ok((StatusCode::CREATED, Json(consumer)))
}

/// `GET /v1/app` — the tenant's consumers, with endpoint / event counts.
async fn list_consumers_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
) -> Result<Json<Vec<store::ConsumerSummary>>, ApiError> {
    Ok(Json(store::list_consumers(&state.pool, &tenant).await?))
}

/// `GET /v1/app/{uid}` — one consumer, tenant-scoped.
async fn get_consumer_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(uid): Path<String>,
) -> Result<Json<dropless_core::model::Consumer>, ApiError> {
    Ok(Json(resolve_consumer(&state, &tenant, &uid).await?))
}

/// `POST /v1/app/{uid}/endpoints` — register an endpoint under a consumer.
async fn create_consumer_endpoint_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(uid): Path<String>,
    Json(body): Json<CreateEndpoint>,
) -> Result<(StatusCode, Json<CreatedEndpoint>), ApiError> {
    let consumer = resolve_consumer(&state, &tenant, &uid).await?;
    let secret = body
        .secret
        .unwrap_or_else(dropless_core::signing::generate_secret);
    let endpoint = store::create_endpoint(
        &state.pool,
        &tenant,
        Some(consumer.id),
        &body.url,
        &secret,
        body.event_types.as_deref(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedEndpoint { endpoint, secret }),
    ))
}

/// `GET /v1/app/{uid}/endpoints` — a consumer's endpoints (secrets hidden).
async fn list_consumer_endpoints_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(uid): Path<String>,
) -> Result<Json<Vec<dropless_core::model::Endpoint>>, ApiError> {
    let consumer = resolve_consumer(&state, &tenant, &uid).await?;
    Ok(Json(
        store::list_endpoints_for_consumer(&state.pool, consumer.id).await?,
    ))
}

/// `POST /v1/app/{uid}/messages` — publish to a consumer. Fans out ONLY to that
/// consumer's endpoints — never another consumer's, never the tenant's own.
async fn create_consumer_message_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(uid): Path<String>,
    Json(body): Json<CreateMessage>,
) -> Response {
    if let Some(retry_after) = state.limiter.check(&tenant) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", format!("{retry_after:.0}"))],
            Json(serde_json::json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
    }
    let consumer = match resolve_consumer(&state, &tenant, &uid).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    match outbox::enqueue_message(
        &state.pool,
        &tenant,
        Some(consumer.id),
        &body.event_type,
        &body.payload,
    )
    .await
    {
        Ok(enq) => {
            state.notifier.ping();
            (
                StatusCode::CREATED,
                Json(CreateMessageResponse {
                    id: enq.event_id,
                    delivery_ids: enq.delivery_ids,
                }),
            )
                .into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// `GET /v1/app/{uid}/messages` — recent events for a consumer (newest first).
async fn list_consumer_messages_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Path(uid): Path<String>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<dropless_core::model::Event>>, ApiError> {
    let consumer = resolve_consumer(&state, &tenant, &uid).await?;
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let offset = q.offset.unwrap_or(0).max(0);
    Ok(Json(
        store::list_events_for_consumer(&state.pool, consumer.id, limit, offset).await?,
    ))
}

// ---- inbound gateway (v0.2) -------------------------------------------------

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// sha256-hex of bytes. Used to derive a replay-safe dedup key from SIGNED
/// material when a provider supplies no in-body event id.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// `POST /ingest/{slug}` — receive a provider webhook. Authenticated by the
/// provider's signature (not an API key); persisted raw-first, deduped on
/// SIGNED material, and bridged into the outbound pipeline.
async fn ingest(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Unknown/disabled source and signature failure return an IDENTICAL 401 so
    // the public endpoint is not a slug-existence or provider oracle.
    let unauthorized = || {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response()
    };

    let source = match store::get_inbound_source_by_slug(&state.pool, &slug).await {
        Ok(Some(s)) if !s.disabled => s,
        Ok(_) => return unauthorized(),
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response(),
    };
    let Ok(provider) = source.provider.parse::<Provider>() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "misconfigured provider").into_response();
    };
    let now = chrono::Utc::now().timestamp();
    let raw = body.as_ref();

    // Verify, and derive the dedup key + event type. The dedup key is ALWAYS
    // bound to signed material (never an unsigned, attacker-mutable header), so a
    // replay of the same signed bytes always collides — closing the replay /
    // fan-out-amplification hole even when the provider's id is absent.
    let (verified, dedup_key, event_type) = match provider {
        Provider::Stripe => {
            let v = inbound::verify_stripe(
                &source.secret,
                header_str(&headers, "stripe-signature").unwrap_or(""),
                raw,
                now,
                300,
            );
            let meta: serde_json::Value =
                serde_json::from_slice(raw).unwrap_or(serde_json::Value::Null);
            // `id`/`type` live inside the signed body.
            let key = meta
                .get("id")
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| sha256_hex(raw));
            let ty = meta
                .get("type")
                .and_then(|x| x.as_str())
                .unwrap_or("stripe.event")
                .to_string();
            (v, key, ty)
        }
        Provider::Github => {
            let v = inbound::verify_github(
                &source.secret,
                header_str(&headers, "x-hub-signature-256").unwrap_or(""),
                raw,
            );
            // X-GitHub-Delivery is NOT signed, so it cannot be trusted for replay
            // protection; hash the signed body instead. (event_type is a label.)
            let ty = header_str(&headers, "x-github-event")
                .unwrap_or("github.event")
                .to_string();
            (v, sha256_hex(raw), ty)
        }
        Provider::Hmac => {
            let id = header_str(&headers, "webhook-id").unwrap_or("");
            let ts = header_str(&headers, "webhook-timestamp")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let sig = header_str(&headers, "webhook-signature").unwrap_or("");
            let v = inbound::verify_hmac(&source.secret, id, ts, raw, sig, now, 300);
            // webhook-id is signed; fall back to a body hash if absent.
            let key = if id.is_empty() {
                sha256_hex(raw)
            } else {
                id.to_string()
            };
            let ty = header_str(&headers, "dropless-event-type")
                .unwrap_or("inbound.event")
                .to_string();
            (v, key, ty)
        }
    };
    if verified.is_err() {
        return unauthorized();
    }

    // Outbound payload: the parsed JSON, or a wrapper if the body isn't JSON.
    // (raw_body is stored byte-exact below — this is only for downstream fan-out.)
    let payload = serde_json::from_slice::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::json!({ "raw": String::from_utf8_lossy(raw) }));

    match inbox::accept(
        &state.pool,
        &source,
        Some(dedup_key.as_str()),
        &event_type,
        raw,
        &payload,
    )
    .await
    {
        Ok(Accepted::Bridged {
            event_id,
            delivery_ids,
            ..
        }) => {
            state.notifier.ping();
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "status": "accepted",
                    "event_id": event_id,
                    "deliveries": delivery_ids.len(),
                })),
            )
                .into_response()
        }
        Ok(Accepted::Duplicate) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "duplicate" })),
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "ingest failed").into_response(),
    }
}

#[derive(Deserialize)]
struct CreateInboundSource {
    slug: String,
    provider: String,
    /// Provider signing secret. Required for stripe/github; a `whsec_…` is
    /// generated for `hmac` if omitted (and returned once).
    secret: Option<String>,
}

#[derive(Serialize)]
struct CreatedInboundSource {
    #[serde(flatten)]
    source: dropless_core::model::InboundSource,
    secret: String,
}

/// `POST /v1/inbound-sources` — register an inbound source for the tenant.
async fn create_inbound_source_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    Json(body): Json<CreateInboundSource>,
) -> Result<(StatusCode, Json<CreatedInboundSource>), ApiError> {
    body.provider.parse::<Provider>()?; // 400 on an unknown provider
    let secret = match body.secret {
        // A too-short secret silently disables auth for the source (anyone can
        // recompute the HMAC), so require real entropy.
        Some(s) if s.len() < 16 => {
            return Err(CoreError::Invalid("secret must be at least 16 characters".into()).into())
        }
        Some(s) => s,
        None => dropless_core::signing::generate_secret(),
    };
    let source =
        store::create_inbound_source(&state.pool, &tenant, &body.slug, &body.provider, &secret)
            .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedInboundSource { source, secret }),
    ))
}

/// `GET /v1/inbound-sources` — the tenant's inbound sources (secrets hidden).
async fn list_inbound_sources_handler(
    State(state): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
) -> Result<Json<Vec<dropless_core::model::InboundSource>>, ApiError> {
    Ok(Json(
        store::list_inbound_sources(&state.pool, &tenant).await?,
    ))
}

// ---- health / readiness -----------------------------------------------------

/// `GET /healthz` — liveness: the process is up and serving (no dependencies).
async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// `GET /readyz` — readiness: ready to serve traffic (database reachable).
async fn readyz(State(state): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => (StatusCode::OK, "ready").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response(),
    }
}

/// `GET /metrics` — minimal Prometheus exposition of delivery counts. Counts
/// are system-wide (cross-tenant), so when `metrics_token` is configured this
/// route requires it; otherwise it is open and must be firewalled.
async fn metrics(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    if let Some(expected) = &state.metrics_token {
        let provided = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim);
        if provided != Some(expected.as_ref()) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    let counts = match store::status_counts(&state.pool).await {
        Ok(c) => c,
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response(),
    };
    let events = store::total_events(&state.pool).await.unwrap_or(0);
    let mut out = String::new();
    out.push_str("# HELP dropless_events_total Events accepted (ingest volume).\n");
    out.push_str("# TYPE dropless_events_total counter\n");
    out.push_str(&format!("dropless_events_total {events}\n"));
    out.push_str("# HELP dropless_deliveries Deliveries by status.\n");
    out.push_str("# TYPE dropless_deliveries gauge\n");
    for (status, count) in counts {
        out.push_str(&format!(
            "dropless_deliveries{{status=\"{status}\"}} {count}\n"
        ));
    }
    (StatusCode::OK, out).into_response()
}

// ---- operator admin console (cross-tenant) ----------------------------------

/// Returns `Some(rejection)` unless the request is an authorized operator.
/// When `ADMIN_TOKEN` is unset the whole admin surface 404s (looks absent).
fn admin_gate(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let Some(expected) = &state.admin_token else {
        return Some(StatusCode::NOT_FOUND.into_response());
    };
    let provided = header_str(headers, "authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    if provided == Some(expected.as_ref()) {
        None
    } else {
        Some((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
    }
}

/// `GET /admin/overview` — per-tenant rollups (operator only).
async fn admin_overview(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(rej) = admin_gate(&state, &headers) {
        return rej;
    }
    match store::admin_tenant_stats(&state.pool).await {
        Ok(stats) => Json(stats).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response(),
    }
}

/// `GET /admin/failures` — recent failed/dead deliveries across all tenants.
async fn admin_failures(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(rej) = admin_gate(&state, &headers) {
        return rej;
    }
    match store::admin_recent_failures(&state.pool, 100).await {
        Ok(f) => Json(f).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response(),
    }
}

/// `GET /admin` — operator console page (404 when admin is disabled).
async fn admin_dashboard(State(state): State<AppState>) -> Response {
    if state.admin_token.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    Html(include_str!("admin.html")).into_response()
}

/// `GET /` — the minimal embedded admin dashboard.
async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

// ---- error mapping ----------------------------------------------------------

/// API error that maps [`CoreError`] onto HTTP status codes.
struct ApiError(StatusCode, String);

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::NotFound(m) => ApiError(StatusCode::NOT_FOUND, m),
            CoreError::Invalid(m) => ApiError(StatusCode::BAD_REQUEST, m),
            // A unique-constraint violation (e.g. a taken slug) is a 409 — but
            // with a generic message, never the raw Postgres text.
            CoreError::Db(sqlx::Error::Database(dbe)) if dbe.is_unique_violation() => {
                ApiError(StatusCode::CONFLICT, "already exists".into())
            }
            // Never leak internal/DB error detail to clients; log it server-side.
            other => {
                tracing::error!(error = %other, "request failed");
                ApiError(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let ApiError(code, message) = self;
        (code, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_hex() {
        let h = hash_key("secret-key");
        assert_eq!(h.len(), 64);
        assert_eq!(h, hash_key("secret-key"));
        assert_ne!(h, hash_key("other"));
    }
}

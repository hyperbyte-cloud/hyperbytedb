# Extension Points

Step-by-step guides for extending HyperbyteDB with new functionality.

---

## Adding a New TimeseriesQL Statement

### Step 1: Define the AST node

In `src/timeseriesql/ast.rs`, add a variant to the `Statement` enum:

```rust
pub enum Statement {
    // existing variants...
    NewStatement(NewStatementData),
}
```

### Step 2: Parse it

In `src/timeseriesql/parser.rs`, add a dispatch case in `parse_statement()`:

```rust
fn parse_statement(input: &str) -> Result<Statement, HyperbytedbError> {
    let first = first_token(input);
    match first.to_uppercase().as_str() {
        "NEW" => parse_new_statement(input),
        // existing cases...
    }
}
```

Implement `parse_new_statement()` following the existing recursive descent patterns.

### Step 3: Execute it

In `src/application/query_service.rs`, add a match arm in `execute_statement()`:

```rust
Statement::NewStatement(data) => {
    // Execute the statement logic using metadata, WAL, or other ports
    // Return StatementResult
}
```

### Step 4: Translate to ClickHouse (if SELECT-like)

If the statement involves data queries, add translation logic in `src/timeseriesql/to_clickhouse.rs`.

### Step 5: Handle cluster replication (if mutating)

If the statement modifies state:
1. Add it to `is_cluster_mutation()` in `src/application/peer_query_service.rs`.
2. Add a `MutationRequest` variant in `src/domain/cluster/types.rs`.
3. Add application logic in `src/adapters/cluster/raft/state_machine.rs`.

---

## Adding a New Points Sink Backend

Time-series data is flushed through `PointsSinkPort` (currently implemented by `ChdbNativeAdapter`).

### Step 1: Implement `PointsSinkPort`

Create a new file in `src/adapters/`:

```rust
#[async_trait]
impl PointsSinkPort for NewSink {
    async fn write_points(/* ... */) -> Result<WriteAck, HyperbytedbError> { /* ... */ }
    // other trait methods...
}
```

### Step 2: Wire it in bootstrap

In `src/bootstrap.rs`, construct your adapter and pass it to `FlushServiceImpl::new(...)`.

### Step 3: Add config

Add config fields in `src/config.rs` and document in `docs/user-guide/configuration.md`.

---

## Adding a New Background Service

Background services follow a consistent pattern. Use `RetentionService` as the simplest reference.

### Step 1: Define the service

```rust
pub struct NewService {
    metadata: Arc<dyn MetadataPort>,
}

impl NewService {
    pub fn new(metadata: Arc<dyn MetadataPort>) -> Self {
        Self { metadata }
    }

    pub async fn run(
        &self,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.do_work().await {
                        tracing::error!("new service error: {}", e);
                        counter!("hyperbytedb_new_service_errors_total").increment(1);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("new service shutting down");
                        break;
                    }
                }
            }
        }
    }

    async fn do_work(&self) -> Result<(), HyperbytedbError> {
        // service logic here
        Ok(())
    }
}
```

### Step 2: Wire in bootstrap and main

In `src/bootstrap.rs` or `src/main.rs`, create and spawn the service:

```rust
let new_svc = Arc::new(NewService::new(metadata.clone()));
let new_shutdown = shutdown_rx.clone();
let new_handle = tokio::spawn(async move {
    new_svc.run(Duration::from_secs(30), new_shutdown).await;
});
// Await the handle during shutdown
```

---

## Adding a New Port

### Step 1: Define the trait

In `src/ports/new_port.rs`:

```rust
#[async_trait]
pub trait NewPort: Send + Sync {
    async fn do_something(&self, input: &str) -> Result<Output, HyperbytedbError>;
}
```

### Step 2: Register the module

Add `pub mod new_port;` to `src/ports/mod.rs`.

### Step 3: Implement and wire

Create the adapter in `src/adapters/` and wire it in `src/bootstrap.rs`.

---

## Adding a New HTTP Endpoint

### Step 1: Create the handler

In `src/adapters/http/` (new file or existing):

```rust
pub async fn handle_new_endpoint(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NewParams>,
) -> Result<impl IntoResponse, HyperbytedbError> {
    // handler logic
    Ok(Json(result))
}
```

### Step 2: Register the route

In `src/adapters/http/router.rs`, add to `build_router()`:

```rust
.route("/new-endpoint", get(handle_new_endpoint))
```

### Step 3: Add any needed state

If the handler needs new shared state, add fields to `AppState` in `router.rs` and populate in `bootstrap.rs`.

---

## Adding a New Adapter for an Existing Port

To replace or add an alternative implementation of an existing port:

1. Create the implementation in `src/adapters/`.
2. Implement the port trait.
3. Add a config option to select the implementation.
4. Wire it in `src/bootstrap.rs` with a match on the config.

The system is designed so that swapping implementations requires changes only in `bootstrap.rs` and config — no business logic changes needed.

---

## See Also

- [Core Modules](core-modules.md) — Module reference
- [Coding Standards](../coding-standards.md) — Code conventions to follow
- [Contributing](../contributing.md) — Review process for changes

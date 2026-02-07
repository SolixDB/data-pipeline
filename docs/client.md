# client.rs — Geyser gRPC client

**Source:** `src/client.rs`

Builds and connects the Yellowstone Geyser gRPC client. All behaviour is described here; config is in [config.md](config.md).

---

## GrpcClient

- **Struct:** holds `Config` (clone used for builder).
- **`new(config: Config)`:** stores config.

---

## connect()

Returns `anyhow::Result<GeyserGrpcClient<impl Interceptor + Clone>>`. Used once per retry attempt in main.

Steps:

1. **Builder:** `GeyserGrpcClient::build_from_shared(config.endpoint)` — endpoint must be a valid http(s) URL. `.max_decoding_message_size(config.max_decoding_message_size)` — 1 GiB by default so large transactions don’t fail decode.
2. **Auth:** If `config.api_token` is set, `.x_token(Some(token))` so the interceptor sends the token (e.g. as header or metadata).
3. **TLS:** If `config.endpoint.to_lowercase().starts_with("https://")`, `.tls_config(ClientTlsConfig::new().with_native_roots())` so the connection is TLS. No client certs; native root store only.
4. **Connect:** `.connect().await` — establishes the channel. Errors (DNS, TLS, connection refused) are wrapped in `anyhow`.

So: one logical “connection” per call; main calls `connect()` each time the retry loop runs (after a disconnect or stream error). The returned client is used to create the subscribe stream; when the stream ends or errors, main drops it and calls `connect()` again after backoff.

---

## Dependencies

- **crate::config::Config** — endpoint, api_token, max_decoding_message_size.
- **yellowstone_grpc_client** — GeyserGrpcClient, ClientTlsConfig, Interceptor.
- **anyhow::Context** — for error context.

No other crate modules.

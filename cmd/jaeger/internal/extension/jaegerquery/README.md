# jaeger_query

This extension implements traditional `jaeger-query` service (including Jaeger UI). Because it is not a part of the collector pipeline, it needs access to a storage backend, which can be potentially shared with an exporter (e.g. in order to implement the `all-in-one` functionality). For this reason it depends on the [jaeger_storage](../jaegerstorage/) extension.

## Configuration

The `jaeger_query` extension connects query listeners (HTTP and gRPC) to configured storage backends from the `jaeger_storage` extension:

```yaml
extensions:
  jaeger_query:
    storage:
      traces: some_storage
      traces_archive: archive_storage # optional
    http:
      endpoint: 0.0.0.0:16686
    grpc:
      endpoint: 0.0.0.0:16685
```

### Listeners and Network Exposure

- **HTTP Listener (`http`):** Serves the Jaeger UI, HTTP trace query APIs (`/api/*`, `/api/v3/*`), and AI/MCP endpoints (`/api/ai/*`). By default, it listens on `:16686` across all network interfaces without TLS or authentication.
- **gRPC Listener (`grpc`):** Serves gRPC query APIs (`api_v2`, `api_v3`, capabilities, reflection). By default, it listens on `:16685`.

### Security and Authentication

In production environments, protect query endpoints against unauthorized access:

- **Reverse Proxy / API Gateway (Recommended):** Place a reverse proxy (e.g. Envoy, NGINX, Traefik) in front of Jaeger Query to terminate TLS, manage authentication (OAuth2, OIDC, mTLS, SSO), and enforce rate limits.
- **Basic Authentication (`basicauth`):** Jaeger ships the OpenTelemetry Collector `basicauth` server authenticator extension. It can be attached to `http.auth.authenticator` to guard all HTTP routes.
- **TLS (`http.tls`, `grpc.tls`):** Configure server certificates (`cert_file`, `key_file`) and minimum TLS version (`1.3` or TLS 1.2 default). Always enable TLS when using basic authentication.

### Multi-Tenancy

Multi-tenancy can be enabled to partition queries by tenant in storage:

```yaml
extensions:
  jaeger_query:
    multi_tenancy:
      enabled: true
      header: x-tenant        # default
      tenants: [team-a, team-b] # optional allowlist
```

When enabled, incoming HTTP and gRPC requests must supply a valid tenant header. Tenant validation is storage data segregation rather than caller authentication.

### AI and MCP Integration

The `ai` configuration block mounts the AI chat gateway and Model Context Protocol (MCP) telemetry tools:

```yaml
extensions:
  jaeger_query:
    ai:
      agent_url: ws://localhost:16688 # ACP WebSocket URL for AI chat sidecar
      mcp: {}                         # Telemetry MCP tools endpoint at /api/ai/mcp/
```

- When `ai.mcp` is present, Jaeger serves telemetry MCP tools at `<basePath>/api/ai/mcp/` on the query HTTP port.
- MCP tools inherit the query port's exposure and have direct read access to trace data.
- To disable MCP, omit the `mcp` block under `ai`.
- For complete details on MCP endpoint exposure, recommended production authentication, disabling instructions, and tenancy behavior, see [mcptools README](./internal/mcptools/README.md).

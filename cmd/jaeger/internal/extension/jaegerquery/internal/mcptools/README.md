# mcptools

This package serves Jaeger's telemetry tools over MCP, `read_skill` among them.

A *skill* is a markdown playbook an AI agent reads before attempting a task —
how to detect an N+1 query pattern, how to walk a failed trace to its root
cause. Agents reach them through `read_skill`, starting at the root `SKILL.md`
and following its links, so a skill is only read when it looks relevant.

The `skills/` directory beside this file holds the skills compiled into the
Jaeger binary. Operators can add their own without rebuilding, via
`ai.mcp.skills_dir` — the rest of this file is for them. For writing the skills
themselves, see [AUTHORING.md](./AUTHORING.md).

## Endpoint Exposure and Trace-Data Access

The `ai.mcp` block enables the telemetry MCP endpoint on the query HTTP server; an empty block is enough:

```yaml
extensions:
  jaeger_query:
    ai:
      mcp: {}
```

It serves streamable HTTP MCP at `<basePath>/api/ai/mcp/` (or `/api/ai/mcp/` when `base_path` is unset) on the **query HTTP port** (default `:16686`).

### Network Exposure

The embedded all-in-one configuration (`cmd/jaeger/internal/all-in-one.yaml`) enables the MCP endpoint by default (`ai: mcp: {}`). Because MCP is intended for remote AI agents (e.g. IDE extensions, Claude Code, Cursor, or remote orchestrators), the query HTTP server binds by default to all network interfaces (`:16686`, matching wildcard `0.0.0.0` or `[::]`) rather than loopback.

In default configurations, the query HTTP server runs with **no TLS and no authenticator**. Any remote client that can route traffic to the query port on the network can dial the MCP endpoint.

### Trace-Data Access

The MCP tools are not a sandboxed or redacted subset of trace data; they call Jaeger's internal `QueryService` directly — the exact same service backing the Jaeger UI and the `/api/` HTTP and gRPC trace query APIs. The registered tools:

- `get_services` — lists service names known to Jaeger.
- `get_span_names` — lists span operations and span kinds for a service.
- `search_traces` — searches traces matching filters.
- `get_span_details` — retrieves detailed span attributes, events, and links.
- `get_trace_errors` — retrieves error logs and error tags across spans.
- `get_trace_topology` — builds trace dependency DAGs.
- `get_critical_path` — computes the critical path of a trace.
- `get_service_dependencies` — inspects service dependency relationships.
- `read_skill` — serves Markdown playbooks.

These tools have complete read access to all trace data in the configured storage backends. Jaeger does not redact span attributes or filter sensitive values before returning tool results to an agent.

Consequently, the MCP endpoint is part of the query API's security boundary and must be protected with appropriate network and authentication controls in production.

## Recommended Production Setup

Do not expose unauthenticated query HTTP listeners directly to untrusted networks. To run MCP safely in production, apply the following recommendations:

### 1. Network Perimeter Controls

- Restrict access to port `:16686` using firewall rules, VPC security groups, or Kubernetes NetworkPolicies so only authorized AI agents, sidecars, or trusted internal networks can reach the port.
- If AI clients or sidecars are co-located on the same host, bind the query HTTP listener specifically to localhost:
  ```yaml
  extensions:
    jaeger_query:
      http:
        endpoint: 127.0.0.1:16686
  ```

### 2. Reverse Proxy or API Gateway (Recommended)

The recommended production architecture places a reverse proxy or API gateway (e.g., Envoy, NGINX, Traefik, or an ingress controller) in front of Jaeger Query:

- **TLS Termination:** The proxy terminates TLS with managed certificates.
- **Authentication & Authorization:** The proxy authenticates remote AI clients using enterprise schemes such as OAuth2, OIDC, SSO, or mutual TLS (mTLS), and enforces authorization policies before forwarding requests to `/api/ai/mcp/`.
- **Rate Limiting:** Protects the query service against aggressive agent polling or denial-of-service.

### 3. In-Process Basic Authentication and TLS

When an external reverse proxy is not used, Jaeger can terminate TLS and enforce HTTP Basic Authentication directly using the shipped OpenTelemetry Collector `basicauth` server authenticator extension (`basicauthextension`):

```yaml
service:
  extensions: [jaeger_storage, basicauth/server, jaeger_query]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger_storage_exporter]

extensions:
  basicauth/server:
    htpasswd:
      file: /etc/jaeger/htpasswd

  jaeger_query:
    http:
      endpoint: 0.0.0.0:16686
      auth:
        authenticator: basicauth/server
      tls:
        cert_file: /etc/jaeger/certs/server.crt
        key_file: /etc/jaeger/certs/server.key
        min_version: "1.3"
    storage:
      traces: some_storage
    ai:
      mcp: {}
```

When `auth.authenticator` is set on `extensions.jaeger_query.http`, the authenticator guards all routes on the query HTTP listener, including `/api/ai/mcp/`.

> **Important:** Basic authentication credentials are sent as Base64-encoded strings on each HTTP request. Never configure `basicauth` without enabling TLS.

### Security Boundaries: Jaeger Controls vs External Controls

- **No in-process RBAC or user identity:** Jaeger does not implement caller identities, user accounts, roles, or permissions, and does not record an audit log of who read a trace.
- **Bearer tokens:** Incoming `Authorization: Bearer` tokens are not validated by Jaeger. If `bearer_token_propagation: true` is configured, tokens are passed through to the storage backend, delegating token validation to that storage system.

## Disabling the MCP Endpoint

If MCP is not needed, disable it to reduce attack surface.

The endpoint is gated strictly by the presence of the `mcp` block under `extensions.jaeger_query.ai`:

### In Custom Configuration

In custom configuration files, simply omit or remove the `mcp` block:

```yaml
extensions:
  jaeger_query:
    ai:
      agent_url: ws://localhost:16688
      # mcp block omitted
```

- **Shared and Turn-Scoped Endpoints:** When `ai.mcp` is omitted, neither the shared `/api/ai/mcp/` endpoint nor the turn-scoped `/api/ai/mcp/<id>/` endpoint is mounted.
- **Impact on AI Chat:** If `agent_url` is still configured, the chat endpoint (`<basePath>/api/ai/chat`) remains mounted for UI chat, but neither telemetry tools nor contextual UI tools will be available over MCP to the AI sidecar.
- **Disabling All AI Surfaces:** If AI chat is also not needed, omit the entire `ai` block from `jaeger_query`. When `ai` is absent, no AI routes (`/api/ai/*`) are registered.

### In Embedded All-in-One

The embedded all-in-one configuration (`cmd/jaeger/internal/all-in-one.yaml`) includes `ai: mcp: {}` by default. When running all-in-one, pass an explicit configuration file via `--config /path/to/config.yaml` that omits `mcp` (or omits `ai`); passing `--config` replaces the embedded configuration.

## Multi-Tenancy and Identity Behavior

### Identity in MCP Sessions

MCP sessions carry **no caller identity**. Jaeger does not authenticate the identity of an AI agent or map MCP sessions to individual user identities or roles.

### Multi-Tenancy Enforcement

When multi-tenancy is enabled on the query extension (`extensions.jaeger_query.multi_tenancy.enabled: true`):

1. **Header Requirement:** Every HTTP request to `/api/ai/mcp/` is checked by Jaeger's tenancy middleware. Callers must provide the configured tenant header (default: `x-tenant`).
   - A missing tenant header returns `401 Unauthorized` (`missing tenant header`).
   - If an allowed tenants list is configured (`multi_tenancy.tenants: [...]`) and the provided tenant is not in the list, Jaeger returns `401 Unauthorized` (`unknown tenant`).
   - If `tenants` is omitted or empty, any non-empty tenant header is accepted.
2. **Context Propagation to Storage:** Once accepted, the tenant value is injected into the request context (`tenancy.WithTenant`) and propagated through `QueryService` to storage queries. The storage backend partitions and returns only trace data belonging to that tenant.

### Streamable HTTP Session State vs Tenancy

Streamable HTTP MCP maintains transport-level session state tracked via the `Mcp-Session-Id` header (expiring after 5 minutes of inactivity). However:

- Tenancy is evaluated and extracted **on each HTTP request**.
- MCP does not bind a session to a single tenant at initialization or enforce session-locked tenant isolation inside the MCP session layer.
- Clients must include the tenant header on every HTTP request.

### Security Limitations

Multi-tenancy in Jaeger is a mechanism for **storage data segregation**, not cryptographic authentication:

- Jaeger does not verify credentials or signatures inside the tenant header.
- In production, upstream infrastructure (such as an API gateway or reverse proxy) must authenticate the caller, determine their authorized tenant, and inject or sanitize the `x-tenant` header before proxying requests to Jaeger.

## Adding installation-specific skills

Point `ai.mcp.skills_dir` at a directory on the query server's disk:

```yaml
extensions:
  jaeger_query:
    ai:
      mcp:                      # skills served over MCP
        skills_dir: /etc/jaeger/skills
```

The all-in-one config is not externally configurable, but the field can still be
set on it with `--set extensions.jaeger_query.ai.mcp.skills_dir={path}`.

Its contents are served under `custom/`, beside the built-in skills:

```
SKILL.md                       # built-in entry point
detect-n-plus-one/SKILL.md     # built-in skill
error-root-cause/SKILL.md      # built-in skill
custom/SKILL.md                # your entry point, mounts <skills_dir>/SKILL.md
custom/your-skill/SKILL.md     # your skill      , mounts <skills_dir>/your-skill/SKILL.md
```

`custom/` is a prefix on the path an agent asks for, not a directory you create:
inside `skills_dir`, your entry point is plain `SKILL.md`.

`<skills_dir>/SKILL.md` is required: an agent reads it first, and without it
nothing below is reachable. It is also your index. An agent chooses what to read
next from the links it finds there and from nothing else, so the line you write
beside each link is what decides whether that skill is ever opened. If your
instructions are short, the entry point can be all you write. Anything longer
belongs in a file of its own that the index links to.

Write those links relative to the **skills root**, not to the linking file's own
directory — an agent passes the link text straight back to `read_skill`:

```markdown
- [your-skill](custom/your-skill/SKILL.md) — one line on when to use it.
```

A link may point at any file, under any name; `SKILL.md` is required only at the
root. The names above are a convention, not a rule.

A skill is markdown. Jaeger serves it as-is and parses nothing inside it:

```markdown
---
name: your-skill
description: One line on what this skill does.
---

# Your Skill

## When this applies
...

## Procedure
1. ...
```

Frontmatter like the block above is optional. It travels well — both the
[Open Knowledge Format](https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing)
and the [agent skills specification](https://agentskills.io/specification) use
one, so a skill written for either stays readable here — but Jaeger reads no
field of it, and a `description` there is no substitute for the line beside the
link in your index.

## Editing skills on a running server

`skills_dir` is read live. Editing a skill's text, or adding a file the index
links to, takes effect on the next `read_skill` call — no restart, no reload
signal. Startup validation is the exception: it runs once, so a `skills_dir`
that breaks after the server started is reported when an agent reads it rather
than by refusing to serve.

Skills are static text, and Jaeger never executes them. What reads them is an
agent, which then decides what to do.

## What Jaeger checks, and what it limits

A `skills_dir` that cannot be opened, or whose `SKILL.md` cannot be read, is
broken configuration, and Jaeger refuses to start rather than quietly serving an
incomplete skill set:

| Startup error | Cause |
| --- | --- |
| `cannot open skills_dir "…"` | Path is missing, or is not a directory |
| `cannot read SKILL.md in skills_dir "…"` | Directory opens, but has no readable entry point |

At read time a bad path is reported back to the agent as
`cannot read "<path>": …` — which is also what every `custom/…` path returns
when no `skills_dir` is configured.

Two limits apply to what is served:

- **Path containment.** The directory is opened with `os.OpenRoot`, so `..`
  traversal and symlinks pointing outside `skills_dir` are refused by the OS
  rather than by a path check that could be tricked.
- **File size.** A served file is capped at 512 KiB; beyond that the reply is cut
  and ends with `file content truncated after 524288 bytes`. The cap is fixed, not
  a config field — a skill anywhere near it is far too long to be useful to an
  agent.

## Who can write to skills_dir

A skill is instructions an agent follows, with your telemetry tools already in
hand. Anyone who can write to `skills_dir` can steer that agent's behaviour
without touching Jaeger's binary or configuration. Treat the directory as part
of the trusted configuration surface: own it by root or the Jaeger service
account, keep it out of paths writable by application deployments, and review
changes to it as you would a config change.

## Checking it works

With an MCP client attached to the endpoint, ask for your entry point directly:

```
read_skill(path="custom/SKILL.md")
```

Your text coming back means the directory is mounted and reachable.
`cannot read "custom/SKILL.md"` means either no `skills_dir` is configured on
this server, or the file is not there under that name.

Note that Jaeger serving a skill and an agent choosing to read it are separate
questions — see [AUTHORING.md](./AUTHORING.md), and
[#9336](https://github.com/jaegertracing/jaeger/issues/9336) for what is known
about the second.

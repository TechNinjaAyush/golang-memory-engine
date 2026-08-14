# Decisions

This file records implementation decisions made for this repository and the reason behind each one.

## 2026-08-09 - Kubernetes manifest for memory engine

### Decision: Use one application manifest in `k8s/memory-engine.yaml`

Reason: The `k8s` folder already existed and `k8s/app.yaml` points Argo CD at the `k8s` path, so putting the runtime Kubernetes resources in `k8s/memory-engine.yaml` keeps the deployment discoverable by Argo CD.

### Decision: Include a `ConfigMap` for non-secret environment variables

Reason: `NATS_URL` and `NEO4J_URI` are required by the Go service and are not secrets. A `ConfigMap` makes them easy to change without rebuilding the container image.

### Decision: Include a Kubernetes `Secret` for Neo4j credentials

Reason: `NEO4J_USERNAME` and `NEO4J_PASSWORD` are required by the Go service. Credentials should not live in plain deployment environment fields. The manifest uses `stringData` with placeholder values so the real password can be supplied before deployment.

### Decision: Use `bolt://neo4j:7687` for `NEO4J_URI`

Reason: The `.env` file already points the service at a Kubernetes-style Neo4j service name, and the Neo4j driver commonly connects over Bolt on port `7687`.

### Decision: Use `nats://nats:4222` for `NATS_URL`

Reason: The `.env` file already uses `nats://nats:4222`, which matches the expected in-cluster service DNS pattern for a NATS service named `nats`.

### Decision: Expose container port `3000`

Reason: The Go server runs Gin with `r.Run(":3000")`, so the Deployment and Service should target port `3000`.

### Decision: Add `/healthz` and `/readyz` HTTP endpoints in `cmd/server/main.go`

Reason: The code already had a `CheckHealth` handler, but it was not registered on the Gin router. Kubernetes HTTP liveness and readiness probes need a real route to call.

### Decision: Use HTTP liveness and readiness probes

Reason: The user requested liveness and readiness probes. HTTP probes are appropriate because the service already starts an HTTP server. The probes use `/healthz` and `/readyz`.

### Decision: Set conservative CPU and memory requests and limits

Reason: The service is a small Go process that consumes messages and writes to Neo4j. `100m` CPU and `128Mi` memory requests give the scheduler a baseline, while `500m` CPU and `512Mi` memory limits provide a practical upper bound.

### Decision: Run with a restricted container security context

Reason: The Dockerfile uses a distroless runtime and `USER nonroot:nonroot`. The Kubernetes security context mirrors that by requiring non-root execution, dropping Linux capabilities, disabling privilege escalation, and using a read-only root filesystem.

### Decision: Use `ClusterIP` Service

Reason: The memory engine appears to be an internal service. A `ClusterIP` service exposes it inside the cluster without creating external access.

### Decision: Use `ghcr.io/techninjaayush/golang-memory-engine:latest` as the image placeholder

Reason: The Argo CD manifest points to `https://github.com/TechNinjaAyush/golang-memory-engine`, so the GHCR image name follows that repository identity. This should be confirmed before production deployment.

## Verification Notes

### Decision: Validate YAML syntax locally

Reason: Full `kubectl` validation could not run in this sandbox because the current kubeconfig tries to access GKE credentials and gcloud cannot write its local credential files here. Local YAML parsing succeeded.

### Decision: Do not force dependency downloads after approval was rejected

Reason: `go test ./...` required a clean Go 1.25 toolchain download because the existing Go module cache has truncated dependency files. Network escalation was requested and rejected, so tests were not forced through another path.

## Working Agreement

### Decision: Ask before changing code

Reason: The user requested approval before future code changes. Going forward, code or manifest edits should be proposed first and made only after the user approves them.

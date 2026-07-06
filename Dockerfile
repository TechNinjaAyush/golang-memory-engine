# Stage 1: Build
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s" \
    -a \
    -installsuffix cgo \
    -o memory-engine ./cmd/server

# Stage 2: Runtime
FROM  alpine:3.19

WORKDIR /app

COPY --from=builder /app/memory-engine .

CMD ["./memory-engine"]
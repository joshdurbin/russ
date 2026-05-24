ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

# Cache module downloads in their own layer so source-only edits don't refetch.
COPY go.mod go.sum ./
RUN go mod download

# Only the parts russ-client needs.
COPY cmd/russ-client/ ./cmd/russ-client/
COPY internal/client/ ./internal/client/

RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' \
    -o /russ-client ./cmd/russ-client

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /russ-client /usr/local/bin/russ-client
ENTRYPOINT ["/usr/local/bin/russ-client"]

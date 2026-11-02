# syntax=docker/dockerfile:1

ARG VERSION=dev
ARG COMMIT=unknown

FROM golang:1.26-alpine AS build
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN mkdir -p /out && \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X walcast/internal/metrics.Version=${VERSION} -X walcast/internal/metrics.Commit=${COMMIT}" \
    -o /out/walcast ./cmd/walcast

FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
ARG COMMIT=unknown

LABEL org.opencontainers.image.title="walcast" \
      org.opencontainers.image.description="Streams PostgreSQL changes to Kafka, NATS JetStream and RabbitMQ" \
      org.opencontainers.image.source="https://github.com/KaanSimsek/walcast" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"

COPY --from=build /out/walcast /usr/local/bin/walcast

USER 65532:65532
EXPOSE 8080 8081
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/walcast"]
CMD ["-config", "/etc/walcast/config.yaml"]

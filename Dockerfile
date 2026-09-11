FROM golang:1.23-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# TARGETARCH is set by buildx per platform in the matrix. Hardcoding amd64 here
# would silently produce an amd64 binary inside an arm64 image, which fails at
# exec time rather than at build time.
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -o /out/conduit ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=builder /out/conduit /conduit

# `conduit migrate` reads these at runtime, so they have to be in the image.
# CONDUIT_POSTGRES_MIGRATIONS_PATH defaults to "migrations" relative to WORKDIR.
COPY --from=builder /src/migrations /migrations

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/conduit"]

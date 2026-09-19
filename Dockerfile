# Build the API and the one-shot verify service from one module.
FROM golang:1.23-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/verify ./cmd/verify \
 && mkdir -p /out/verify-state

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/verify /app/verify
# Pre-create the state mountpoint owned by nonroot so the named volume created
# from this path inherits the ownership and the verify service can persist its
# restart-phase state without running as root.
COPY --from=build --chown=nonroot:nonroot /out/verify-state /verify-state
USER nonroot:nonroot

# Default role is the API; compose overrides the entrypoint for verify.
ENTRYPOINT ["/app/api"]

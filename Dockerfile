# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/vaultfs ./cmd/vaultfs

FROM alpine:3.19

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /out/vaultfs /app/vaultfs

# Default port; each service overrides VAULTFS_PORT.
EXPOSE 8001

ENTRYPOINT ["/app/vaultfs"]

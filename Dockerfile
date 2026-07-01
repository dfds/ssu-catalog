FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -o /app/ssu-catalog ./cmd/main.go

FROM alpine:3.21
RUN adduser -D -u 1000 appuser
USER appuser
COPY --from=build /app/ssu-catalog /app/ssu-catalog
EXPOSE 8080 9090
CMD ["/app/ssu-catalog"]

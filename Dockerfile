FROM golang:1.23.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o myapp .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/myapp .

EXPOSE 8081

CMD ["./myapp"]
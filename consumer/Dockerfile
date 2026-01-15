FROM golang:1.25 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go mod tidy

RUN go build -o /app/consumer-app .

FROM ubuntu:latest

WORKDIR /root/

COPY --from=builder /app/consumer-app .

CMD [ "./consumer-app" ]

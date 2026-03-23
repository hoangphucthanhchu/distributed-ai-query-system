FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/consumer /usr/local/bin/consumer
USER nobody
ENTRYPOINT ["/usr/local/bin/consumer"]

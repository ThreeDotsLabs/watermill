FROM golang:1.23 AS builder

COPY . /src
WORKDIR /src/

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /main .

FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=builder /main /main
CMD ["/main"]

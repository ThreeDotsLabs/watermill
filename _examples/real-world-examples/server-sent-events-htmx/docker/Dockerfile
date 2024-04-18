FROM golang:1.22

RUN go install github.com/cespare/reflex@latest

COPY reflex.conf /

ENTRYPOINT ["/go/bin/reflex", "-c", "/reflex.conf"]

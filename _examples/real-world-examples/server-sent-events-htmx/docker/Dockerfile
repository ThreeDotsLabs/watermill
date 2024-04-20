FROM golang:1.22

RUN go install github.com/cespare/reflex@latest
RUN go install github.com/a-h/templ/cmd/templ@latest

COPY reflex.conf /

ENTRYPOINT ["/go/bin/reflex", "-c", "/reflex.conf"]

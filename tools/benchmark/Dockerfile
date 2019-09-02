FROM golang:1.12
WORKDIR /app
COPY . /app
RUN go build -o benchmark ./cmd
CMD ["./benchmark"]

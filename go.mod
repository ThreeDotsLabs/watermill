module github.com/ThreeDotsLabs/watermill

require (
	cloud.google.com/go v0.36.0
	github.com/Shopify/sarama v1.21.0
	github.com/cenkalti/backoff v2.1.1+incompatible
	github.com/go-chi/chi v4.0.2+incompatible
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.2.1-0.20190205222052-c823c79ea157
	github.com/google/uuid v1.1.1
	github.com/hashicorp/go-multierror v1.0.0
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/go-nats-streaming v0.4.0
	github.com/nats-io/nkeys v0.0.2 // indirect
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/oklog/ulid v1.3.1
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.2
	github.com/renstrom/shortuuid v3.0.0+incompatible
	github.com/streadway/amqp v0.0.0-20190225234609-30f8ed68076e
	github.com/stretchr/testify v1.3.0
	google.golang.org/api v0.1.0
	google.golang.org/grpc v1.19.0
)

replace sourcegraph.com/sourcegraph/go-diff v0.5.1 => github.com/sourcegraph/go-diff v0.5.1
replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190409202823-959b441ac422

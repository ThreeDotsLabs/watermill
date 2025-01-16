module main

require (
	github.com/ThreeDotsLabs/watermill v1.4.4
	github.com/ThreeDotsLabs/watermill-aws v1.0.0
	github.com/aws/aws-sdk-go-v2 v1.33.0
	github.com/aws/aws-sdk-go-v2/service/sns v1.33.12
	github.com/aws/aws-sdk-go-v2/service/sqs v1.37.8
	github.com/aws/smithy-go v1.22.1
	github.com/samber/lo v1.47.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.28 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/lithammer/shortuuid/v3 v3.0.7 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/text v0.21.0 // indirect
)

go 1.23

toolchain go1.23.4

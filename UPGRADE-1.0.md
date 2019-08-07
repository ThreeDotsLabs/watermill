# TODO

Migrating Pub/Subs

	find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/(amqp|googlecloud|http|io|kafka|nats|sql)/github.com\/ThreeDotsLabs\/watermill-\1\/pkg\/\1/" "{}" +;
	find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/gochannel/github\.com\/ThreeDotsLabs\/watermill\/pubsub\/gochannel/" "{}" +;

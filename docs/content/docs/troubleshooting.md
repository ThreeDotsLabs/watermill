+++
title = "Troubleshooting"
description = "When something goes wrong"
weight = -90
draft = false
toc = true
bref = "When something goes wrong"
type = "docs"
+++

### Logging

In most cases, you will find the answer to your problem in the logs.
Watermill offers a significant amount of logs on different severity levels.

If you are using `StdLoggerAdapter`, just change `debug`, and `trace` options to true:

{{< highlight >}}
logger := watermill.NewStdLogger(true, true)
{{< /highlight >}}

### Debugging Pub/Sub tests

#### Running single tests

{{< highlight >}}
make up
go test -v ./... -run TestPublishSubscribe/TestContinueAfterSubscribeClose
{{< /highlight >}}

#### grep is your friend

Every test case that is executed, have a unique UUID of the test case. It is used in the topic name.
Thanks to that, you can easily grep the output of the test.
It gives you very detailed information about test execution.

{{< highlight >}}
> go test -v ./... > test.out

> less test.out

// ...

--- PASS: TestPublishSubscribe (0.00s)
    --- PASS: TestPublishSubscribe/TestPublishSubscribe (2.38s)
        --- PASS: TestPublishSubscribe/TestPublishSubscribe/81eeb56c-3336-4eb9-a0ac-13abda6f38ff (2.38s)
{{< /highlight >}}


{{< highlight >}}
cat test.out | grep 81eeb56c-3336-4eb9-a0ac-13abda6f38ff | less

[watermill] 2020/08/18 14:51:46.283366 subscriber.go:300:       level=TRACE msg="Msg acked" message_uuid=5c920330-5075-4870-8d86-9013771eee78 provider=google_cloud_pubsub subscription_name=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff topic=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff
[watermill] 2020/08/18 14:51:46.283405 subscriber.go:300:       level=TRACE msg="Msg acked" message_uuid=46e04a08-994e-4c04-afff-7fd42fd67f95 provider=google_cloud_pubsub subscription_name=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff topic=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff
2020/08/18 14:51:46 all messages (100/100) received in bulk read after 110.04155ms of 45s (test ID: 81eeb56c-3336-4eb9-a0ac-13abda6f38ff)
[watermill] 2020/08/18 14:51:46.284569 subscriber.go:186:       level=DEBUG msg="Closing message consumer" provider=google_cloud_pubsub subscription_name=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff topic=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff
[watermill] 2020/08/18 14:51:46.284828 subscriber.go:300:       level=TRACE msg="Msg acked" message_uuid=2f409208-d4d2-46f6-b6b9-afb1aea0e59f provider=google_cloud_pubsub subscription_name=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff topic=topic_81eeb56c-3336-4eb9-a0ac-13abda6f38ff
        --- PASS: TestPublishSubscribe/TestPublishSubscribe/81eeb56c-3336-4eb9-a0ac-13abda6f38ff (2.38s)
{{< /highlight >}}

### I have a deadlock

When running locally, you can send a `SIGQUIT` to the running process:

- `CTRL + \` on Linux
- `kill -s SIGQUIT [pid]` on other UNIX systems

This will kill the process and print all goroutines along with lines on which they have stopped.

```
SIGQUIT: quit
PC=0x45e7c3 m=0 sigcode=128

goroutine 1 [runnable]:
github.com/ThreeDotsLabs/watermill/pubsub/gochannel.(*GoChannel).sendMessage(0xc000024100, 0x7c5250, 0xd, 0xc000872d70, 0x0, 0x0)
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/pubsub/gochannel/pubsub.go:83 +0x36a
github.com/ThreeDotsLabs/watermill/pubsub/gochannel.(*GoChannel).Publish(0xc000024100, 0x7c5250, 0xd, 0xc000098530, 0x1, 0x1, 0x0, 0x0)
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/pubsub/gochannel/pubsub.go:53 +0x6d
main.publishMessages(0x7fdf7a317000, 0xc000024100)
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/pubsubs/go-channel/main.go:43 +0x1ec
main.main()
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/pubsubs/go-channel/main.go:36 +0x20a

// ...
```

When running in production and you don't want to kill the entire process, a better idea is to use [pprof](https://golang.org/pkg/net/http/pprof/).

You can visit [http://localhost:6060/debug/pprof/goroutine?debug=1](http://localhost:6060/debug/pprof/goroutine?debug=1)
on your local machine to see all goroutines status.


```
goroutine profile: total 5
1 @ 0x41024c 0x6a8311 0x6a9bcb 0x6a948d 0x7028bc 0x70260a 0x42f187 0x45c971
#	0x6a8310	github.com/ThreeDotsLabs/watermill.LogFields.Add+0xd0							/home/example/go/src/github.com/ThreeDotsLabs/watermill/log.go:15
#	0x6a9bca	github.com/ThreeDotsLabs/watermill/pubsub/gochannel.(*GoChannel).sendMessage+0x6fa	/home/example/go/src/github.com/ThreeDotsLabs/watermill/pubsub/gochannel/pubsub.go:75
#	0x6a948c	github.com/ThreeDotsLabs/watermill/pubsub/gochannel.(*GoChannel).Publish+0x6c		/home/example/go/src/github.com/ThreeDotsLabs/watermill/pubsub/gochannel/pubsub.go:53
#	0x7028bb	main.publishMessages+0x1eb										/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/pubsubs/go-channel/main.go:43
#	0x702609	main.main+0x209												/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/pubsubs/go-channel/main.go:36
#	0x42f186	runtime.main+0x206											/usr/lib/go/src/runtime/proc.go:201

// ...
```

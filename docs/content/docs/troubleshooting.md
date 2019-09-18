+++
title = "Troubleshooting"
description = "When something goes wrong"
weight = 0
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
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/getting-started/go-channel/main.go:43 +0x1ec
main.main()
	/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/getting-started/go-channel/main.go:36 +0x20a

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
#	0x7028bb	main.publishMessages+0x1eb										/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/getting-started/go-channel/main.go:43
#	0x702609	main.main+0x209												/home/example/go/src/github.com/ThreeDotsLabs/watermill/docs/src-link/_examples/getting-started/go-channel/main.go:36
#	0x42f186	runtime.main+0x206											/usr/lib/go/src/runtime/proc.go:201

// ...
```

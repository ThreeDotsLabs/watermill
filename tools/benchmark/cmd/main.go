package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ThreeDotsLabs/watermill/tools/benchmark/pkg"
)

var pubsubFlag = flag.String("pubsub", "", "")

func main() {
	flag.Parse()

	fmt.Printf("starting benchmark, pubsub: %s\n", *pubsubFlag)

	_, subResults, err := pkg.RunBenchmark(*pubsubFlag)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  count:       %9d\n", subResults.Count)
	fmt.Printf("  1-min rate:  %12.2f\n", subResults.Rate1)
	fmt.Printf("  5-min rate:  %12.2f\n", subResults.Rate5)
	fmt.Printf("  15-min rate: %12.2f\n", subResults.Rate15)
	fmt.Printf("  mean rate:   %12.2f\n", subResults.RateMean)
}

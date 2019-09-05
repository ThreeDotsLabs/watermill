package main

type Event interface{}

type eventSourced struct {
	version int64
	changes []Event
}

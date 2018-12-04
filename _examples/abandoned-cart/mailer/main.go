package main

import "time"

type abandonedCartEvent struct {
	UserEmail string    `json:"user_email"`
	Time      time.Time `json:"time"`
}

func main() {

}

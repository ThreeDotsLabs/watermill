package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// handler receives the webhook requests and logs them in stdout.
func handler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fmt.Printf(
		"[%s] %s %s: %s\n\n",
		time.Now().String(),
		r.Method,
		r.URL.String(),
		string(body),
	)
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8001", http.DefaultServeMux)
}

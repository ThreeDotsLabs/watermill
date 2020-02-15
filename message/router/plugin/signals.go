package plugin

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThreeDotsLabs/watermill/message"
)

// SignalsHandler is a plugin that kills the router after SIGINT or SIGTERM is sent to the process.
func SignalsHandler(r *message.Router) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		r.Logger().Info(fmt.Sprintf("Received %s signal, closing\n", sig), nil)

		err := r.Close()
		if err != nil {
			r.Logger().Error("Router close failed", err, nil)
		}
	}()
	return nil
}

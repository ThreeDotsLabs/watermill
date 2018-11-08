package plugin

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThreeDotsLabs/watermill/message"
)

func SignalsHandler(r *message.Router) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		r.Logger.Info(fmt.Sprintf("Received %s signal, closing\n", sig), nil)

		r.Close()
	}()
	return nil
}

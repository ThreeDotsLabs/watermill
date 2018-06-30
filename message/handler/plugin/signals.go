package plugin

import (
	"os/signal"
	"syscall"
	"fmt"
	"os"
	"github.com/roblaszczak/gooddd/message/handler"
)

func SignalsHandler(handler *handler.Handler) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		handler.Logger.Info(fmt.Sprintf("Received %s signal, closing\n", sig), nil)

		handler.Close()
	}()
	return nil
}

package plugin

import (
	"os/signal"
	"syscall"
	"fmt"
	"os"
	"github.com/roblaszczak/gooddd/message/handler"
)

func SignalsHandler(router *handler.Router) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		router.Logger.Info(fmt.Sprintf("Received %s signal, closing\n", sig), nil)

		router.Close()
	}()
	return nil
}

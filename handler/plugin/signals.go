package plugin

import (
	"github.com/roblaszczak/gooddd/handler"
	"os/signal"
	"syscall"
	"fmt"
	"os"
)

func SignalsHandler(router *handler.Router) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Printf("received %s signal, closing\n", sig)

		router.Close()
	}()
	return nil
}

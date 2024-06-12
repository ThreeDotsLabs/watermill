package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/kelseyhightower/envconfig"
	_ "github.com/lib/pq"
)

type config struct {
	Port            int    `envconfig:"PORT" required:"true"`
	DatabaseURL     string `envconfig:"DATABASE_URL" required:"true"`
	PubSubProjectID string `envconfig:"PUBSUB_PROJECT_ID" required:"true"`
}

func main() {
	var cfg config
	err := envconfig.Process("", &cfg)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		panic(err)
	}

	err = MigrateDB(db)
	if err != nil {
		panic(err)
	}

	repo := NewRepository(db)

	routers, err := NewRouters(cfg, repo)
	if err != nil {
		panic(err)
	}

	go func() {
		err := routers.EventsRouter.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		err := routers.SSERouter.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		// This goroutine simulates some events being published in the background
		ctx := context.Background()
		for {
			postID := 1 + rand.Intn(2)
			if rand.Intn(2) == 0 {
				_ = routers.EventBus.Publish(ctx, PostViewed{
					PostID: postID,
				})
			} else {
				_ = routers.EventBus.Publish(ctx, PostReactionAdded{
					PostID:     postID,
					ReactionID: allReactions[rand.Intn(len(allReactions))].ID,
				})
			}
			time.Sleep(3 * time.Second)
		}
	}()

	handler := NewHandler(repo, routers.EventBus, routers.SSERouter)

	err = handler.Start(fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		panic(err)
	}
}

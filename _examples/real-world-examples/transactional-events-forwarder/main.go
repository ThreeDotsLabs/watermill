package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	driver "github.com/go-sql-driver/mysql"
)

const (
	projectID             = "transactional-events"
	forwarderSQLTopic     = "eventsToForward"
	googleCloudEventTopic = "lottery-concluded"

	simulatedErrorProbability = 0.5
)

var (
	logger = watermill.NewStdLogger(false, false)
	db     = createDB()
)

type LotteryConcludedEvent struct {
	LotteryID int `json:"lottery_id"`
}

func main() {
	// Setup the Forwarder component so it takes messages from MySQL subscription and pushes them to Google Pub/Sub.
	sqlSubscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		logger,
	)
	expectNoErr(err)

	gcpPublisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: projectID,
		},
		logger,
	)
	expectNoErr(err)

	fwd, err := forwarder.NewForwarder(sqlSubscriber, gcpPublisher, logger, forwarder.Config{
		ForwarderTopic: forwarderSQLTopic,
	})
	expectNoErr(err)

	go func() {
		err := fwd.Run(context.Background())
		expectNoErr(err)
	}()

	go runLotteryService(logger)
	go runPrizeSenderService(logger)

	time.Sleep(time.Second * 60)
}

// Lottery service picks a random user at fixed intervals and makes him win a lottery.
func runLotteryService(logger watermill.LoggerAdapter) {
	logger = logger.With(watermill.LogFields{"service": "lottery"})

	// We'd like to persist in a database that for a given lottery id, a drawn user is a winner.
	// At the same time, we want to emit an event that will tell the rest of the system this happened.
	// We could approach implementing this handler in at least 3 ways:
	availableServiceHandlers := []func(int, string, watermill.LoggerAdapter) error{
		publishEventAndPersistData,
		persistDataAndPublishEvent,
		persistDataAndPublishEventInTransaction,
	}

	users := []string{"Mike", "Dwight", "Jim", "Pamela"}
	lotteryID := 1
	for range time.Tick(time.Second * 5) {
		pickedUser := users[rand.Intn(len(users))]
		logger := logger.With(watermill.LogFields{"user": pickedUser, "lottery_id": lotteryID})
		logger.Info("User has been picked as a winner", nil)

		pickedHandler := availableServiceHandlers[rand.Intn(len(availableServiceHandlers))]
		err := pickedHandler(lotteryID, pickedUser, logger)
		if err != nil {
			logger.Error("Handler failed", err, nil)
		}

		lotteryID++
	}
}

// 1. Publishes event to Google Cloud Pub/Sub first, then stores data in MySQL.
func publishEventAndPersistData(lotteryID int, pickedUser string, logger watermill.LoggerAdapter) error {
	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: projectID,
		},
		logger,
	)
	if err != nil {
		return err
	}
	event := LotteryConcludedEvent{LotteryID: lotteryID}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = publisher.Publish(googleCloudEventTopic, message.NewMessage(watermill.NewULID(), payload))
	if err != nil {
		return err
	}

	// In case this fails, we have an event emitted, but no data persisted yet.
	if err = simulateError(); err != nil {
		logger.Error("Failed to persist data", err, nil)
		return err
	}

	_, err = db.Exec(`INSERT INTO lotteries (lottery_id, winner) VALUES(?, ?)`, lotteryID, pickedUser)
	if err != nil {
		return err
	}

	return nil
}

// 2. Persists data to MySQL first, then publishes an event straight to Google Cloud Pub/Sub.
func persistDataAndPublishEvent(lotteryID int, pickedUser string, logger watermill.LoggerAdapter) error {
	_, err := db.Exec(`INSERT INTO lotteries (lottery_id, winner) VALUES(?, ?)`, lotteryID, pickedUser)
	if err != nil {
		return err
	}

	var publisher message.Publisher
	publisher, err = googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: projectID,
		},
		logger,
	)
	if err != nil {
		return err
	}

	event := LotteryConcludedEvent{LotteryID: lotteryID}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// In case this fails, we have data persisted, but no event emitted.
	if err = simulateError(); err != nil {
		logger.Error("Failed to emit event", err, nil)
		return err
	}

	err = publisher.Publish(googleCloudEventTopic, message.NewMessage(watermill.NewULID(), payload))
	if err != nil {
		return err
	}

	return nil
}

// 3. Persists data in MySQL and emits an event through MySQL to Google Cloud Pub/Sub, all in one transaction.
func persistDataAndPublishEventInTransaction(lotteryID int, pickedUser string, logger watermill.LoggerAdapter) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			logger.Info("Rolling transaction back due to error", watermill.LogFields{"error": err.Error()})
			// In case of an error, we're 100% sure that thanks to MySQL transaction rollback, we won't have any of the undesired situations:
			// - event is emitted, but no data is persisted,
			// - data is persisted, but no event is emitted.
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(`INSERT INTO lotteries (lottery_id, winner) VALUES(?, ?)`, lotteryID, pickedUser)
	if err != nil {
		return err
	}

	var publisher message.Publisher
	publisher, err = sql.NewPublisher(
		tx,
		sql.PublisherConfig{
			SchemaAdapter: sql.DefaultMySQLSchema{},
		},
		logger,
	)
	if err != nil {
		return err
	}

	// Decorate publisher so it wraps an event in an envelope understood by the Forwarder component.
	publisher = forwarder.NewPublisher(publisher, forwarder.PublisherConfig{
		ForwarderTopic: forwarderSQLTopic,
	})

	// Publish an event announcing the lottery winner. Please note we're publishing to a Google Cloud topic here,
	// while using decorated MySQL publisher.
	event := LotteryConcludedEvent{LotteryID: lotteryID}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = publisher.Publish(googleCloudEventTopic, message.NewMessage(watermill.NewULID(), payload))
	if err != nil {
		return err
	}

	return nil
}

// PrizeSender service listens to UserWonLottery events and sends a prize straight to the user that has won.
func runPrizeSenderService(logger watermill.LoggerAdapter) {
	logger = logger.With(watermill.LogFields{"service": "prize_sender"})
	ctx := context.Background()
	googleCloudSubscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID: projectID,
		},
		logger,
	)
	expectNoErr(err)

	events, err := googleCloudSubscriber.Subscribe(ctx, googleCloudEventTopic)
	for rawEvent := range events {
		event := LotteryConcludedEvent{}
		err := json.Unmarshal(rawEvent.Payload, &event)
		expectNoErr(err)

		rawEvent.Ack()

		logger := logger.With(watermill.LogFields{"lottery_id": event.LotteryID})

		row := db.QueryRow("SELECT winner FROM lotteries WHERE lottery_id=?", event.LotteryID)
		var winner string
		err = row.Scan(&winner)
		if err != nil {
			logger.Error("Could not get lottery winner", err, nil)
			continue
		}

		logger.Info("Sending a prize to the winner", watermill.LogFields{
			"winner":     winner,
			"lottery_id": event.LotteryID,
		})
	}
}

func expectNoErr(err error) {
	if err != nil {
		log.Fatalf("expected no error, got: %s", err)
	}
}

func simulateError() error {
	if simulatedErrorProbability >= rand.Float64() {
		return errors.New("simulated error occurred")
	}

	return nil
}

func createDB() *stdSQL.DB {
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "mysql"
	conf.DBName = "watermill"

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	expectNoErr(err)

	err = db.Ping()
	expectNoErr(err)

	_, err = db.Exec(`DROP TABLE IF EXISTS lotteries`)
	expectNoErr(err)

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS lotteries (
    lottery_id INT NOT NULL PRIMARY KEY,
	winner VARCHAR(255) NOT NULL 
) ENGINE=INNODB;
`)
	expectNoErr(err)

	return db
}

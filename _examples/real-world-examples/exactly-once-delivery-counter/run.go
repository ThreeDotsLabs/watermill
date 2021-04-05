package main

import (
	stdSQL "database/sql"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const messagesCount = 5000

// at these messages we will restart MySQL
var restartMySQLAt = map[int]struct{}{
	50:   {},
	1000: {},
	1500: {},
	3000: {},
}

// at these messages we will restart counter worker
var restartWorkerAt = map[int]struct{}{
	100:  {},
	1500: {},
	1600: {},
	3000: {},
}

const senderGoroutines = 5

func main() {
	db := createDB()
	counterUUID := uuid.New().String()

	wg := &sync.WaitGroup{}
	wg.Add(messagesCount)

	bar := pb.StartNew(messagesCount)

	// sending value to sendCounter counter HTTP call
	sendCounter := make(chan struct{}, 0)
	go func() {
		for i := 0; i < messagesCount; i++ {
			sendCounter <- struct{}{}

			// let's challenge exactly-once delivery a bit
			// normally it should trigger re-delivery of the message
			if _, ok := restartMySQLAt[i]; ok {
				restartMySQL()
			}
			if _, ok := restartWorkerAt[i]; ok {
				restartWorker()
			}
		}
		close(sendCounter)
	}()

	for i := 0; i < senderGoroutines; i++ {
		go func() {
			for range sendCounter {
				sendCountRequest(counterUUID)
				wg.Done()
				bar.Increment()
			}
		}()
	}

	wg.Wait()
	bar.Finish()

	timeout := time.Now().Add(time.Second * 30)

	fmt.Println("checking counter with DB, expected count:", messagesCount)

	matchedOnce := true

	for {
		if time.Now().After(timeout) {
			fmt.Println("timeout")
			break
		}

		dbCounterValue, err := getDbCounterValue(db, counterUUID)
		if err != nil {
			fmt.Println("err:", err)
			continue
		}

		fmt.Println("db counter value", dbCounterValue)
		if dbCounterValue == messagesCount {
			if !matchedOnce {
				// let's ensure that nothing new will arrive
				matchedOnce = true
				time.Sleep(time.Second * 2)
				continue
			} else {
				fmt.Println("expected counter value is matching DB value")
				break
			}
		}

		time.Sleep(time.Second)
	}
}

func getDbCounterValue(db *stdSQL.DB, counterUUID string) (int, error) {
	var dbCounterValue int
	row := db.QueryRow("SELECT value from counter WHERE id = ?", counterUUID)

	if err := row.Scan(&dbCounterValue); err != nil {
		return 0, err
	}

	return dbCounterValue, nil
}

func restartWorker() {
	fmt.Println("restarting worker")
	err := exec.Command("docker-compose", "restart", "worker").Run()
	if err != nil {
		fmt.Println("restarting worker failed", err)
	}
}

func restartMySQL() {
	fmt.Println("restarting mysql")
	err := exec.Command("docker-compose", "restart", "mysql").Run()
	if err != nil {
		fmt.Println("restarting mysql failed", err)
	}
}

func sendCountRequest(counterUUID string) {
	for {
		resp, err := http.Post("http://localhost:8080/count/"+counterUUID, "", nil)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			break
		}
	}
}

func createDB() *stdSQL.DB {
	conf := mysql.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "localhost"
	conf.DBName = "example"

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	return db
}

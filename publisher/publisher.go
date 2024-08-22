package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	credentialsFile := os.Getenv("NATS_CREDENTIALS_FILE_PATH")
	subjectName := os.Args[1]

	nc, err := nats.Connect("connect.ngs.global",
                   	nats.UserCredentials(credentialsFile),
                   	nats.Name(fmt.Sprintf("publisher_%s", subjectName)))
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_, err := js.Publish(context.Background(), subjectName, []byte(fmt.Sprintf("order-%d", i)))
		if err != nil {
			log.Println(err)
		}
		log.Println("published order no.", i)
		time.Sleep(1 * time.Second)
	}
}
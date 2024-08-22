package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	credentialsFile := os.Getenv("NATS_CREDENTIALS_FILE_PATH")
	dlqStreamName := os.Args[1]
	originalStreamName := os.Args[2]

	nc, err := nats.Connect("connect.ngs.global",
		nats.UserCredentials(credentialsFile),
		nats.Name(fmt.Sprintf("dlq_%s", originalStreamName)))
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consumer, err := js.CreateOrUpdateConsumer(ctx, dlqStreamName, jetstream.ConsumerConfig{

		Name:        "unprocessed_order_handler",
		Durable:     "unprocessed_order_handler",
		Description: "handle unprocessed jobs",
		BackOff: []time.Duration{
			5 * time.Second,
		},
		MaxDeliver: 2,
		AckPolicy:  jetstream.AckExplicitPolicy,
	})

	if err != nil {
		log.Fatal(err)
	}

	originalStream, err := js.Stream(ctx, originalStreamName)

	type UnprocessedOrderEvent struct {
		Stream    string `json:"stream"`
		Consumer  string `json:"consumer"`
		StreamSeq int    `json:"stream_seq"`
	}

	c, err := consumer.Consume(func(msg jetstream.Msg) {
		var event UnprocessedOrderEvent

		err = json.Unmarshal(msg.Data(), &event)

		if err != nil {
			log.Fatal(err)
		}

		order, err := originalStream.GetMsg(ctx, uint64(event.StreamSeq))

		if err != nil {
			log.Fatal(err)
		}

		log.Println("reprocessing order", string(order.Data))
		msg.Ack()

	})

	if err != nil {
		log.Fatal(err)
	}

	defer c.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

}

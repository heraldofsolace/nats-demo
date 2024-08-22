package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	credentialsFile := os.Getenv("NATS_CREDENTIALS_FILE_PATH")
	streamName := os.Args[1]
	subjectFilter := os.Args[2]

	nc, err := nats.Connect("connect.ngs.global",
                   	nats.UserCredentials(credentialsFile),
                   	nats.Name(fmt.Sprintf("consumer_%s", streamName)))
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

	consumer, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{

		Name:    	"processor",
		Durable: 	"processor",
		Description: "Orders processor",
		BackOff: []time.Duration{
		  5 * time.Second,
		},
		MaxDeliver:	2,
		FilterSubject: subjectFilter,
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	if err != nil {
		log.Fatal(err)
	}

	c, err := consumer.Consume(func(msg jetstream.Msg) {

		meta, err := msg.Metadata()
	 
		if err != nil {
		  log.Printf("Error getting metadata: %s\n", err)
		  return
		}

	 
		if rand.Intn(10) < 5 {
	 
		  log.Println("error processing", string(msg.Data()))
	 
		  if meta.NumDelivered == 2 {
			log.Println(string(msg.Data()), "will be processed via DLQ")
		  }
		  return
		}
	 
		log.Println("received", string(msg.Data()), "from stream sequence #", meta.Sequence.Stream)
		time.Sleep(10 * time.Millisecond)
	 
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
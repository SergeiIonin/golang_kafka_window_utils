// package main

// import (
// 	"context"
// 	"log"
// 	"sync"

// 	"github.com/segmentio/kafka-go"
// )

// func main() {
// 	brokers := []string{"localhost:9092"}

// 	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "timewindows", 0)
// 	if err != nil {
// 		log.Fatal("failed to dial leader:", err)
// 	}

// 	conn.CreateTopics(kafka.TopicConfig{
// 		Topic:             "timewindows",
// 		NumPartitions:     1,
// 		ReplicationFactor: 1,
// 	})

// 	//produce("timewindows", brokers)

// 	wg := &sync.WaitGroup{}
// 	wg.Add(3)

// 	go consume("timewindows", brokers, wg)

// 	wg.Wait()

// }

// func produce(topic string, brokers []string) {
// 	// create kafka writer
// 	w := kafka.NewWriter(kafka.WriterConfig{
// 		Brokers:  brokers,
// 		Topic:    topic,
// 		Balancer: &kafka.LeastBytes{},
// 	})

// 	err := w.WriteMessages(context.Background(),
// 		kafka.Message{
// 			Key:   []byte("Key-A"),
// 			Value: []byte("Hello World!"),
// 		},
// 		kafka.Message{
// 			Key:   []byte("Key-B"),
// 			Value: []byte("One!"),
// 		},
// 		kafka.Message{
// 			Key:   []byte("Key-C"),
// 			Value: []byte("Two!"),
// 		},
// 	)
// 	if err != nil {
// 		log.Fatal("failed to write messages:", err)
// 	}

// 	if err := w.Close(); err != nil {
// 		log.Fatal("failed to close writer:", err)
// 	}
// }

// func consume(topic string, brokers []string, wg *sync.WaitGroup) {
// 	// create kafka reader
// 	r := kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:   brokers,
// 		Topic:     topic,
// 		Partition: 0,
// 		MinBytes:  10e3, // 10KB
// 		MaxBytes:  10e6, // 10MB
// 	})

// 	for {
// 		m, err := r.ReadMessage(context.Background())
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		wg.Done()
// 		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
// 	}

// 	if err := r.Close(); err != nil {
// 		log.Fatal("failed to close reader:", err)
// 	}

// }

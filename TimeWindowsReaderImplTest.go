package main

import (
	"GoKafkaWindowFuncs/timewindows"
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// todo move it to the proper test
func main() {
	brokers := []string{"localhost:9092"}

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "timewindows", 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.CreateTopics(kafka.TopicConfig{
		Topic:             "timewindows",
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	conn.CreateTopics(kafka.TopicConfig{
		Topic:             "timewindows_aggregated",
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	readerConfig := kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "timewindows",
		// assign the default partition
		Partition: 0,
		// start reading from the earliest possible offset
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}

	writerConfig := kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows_aggregated",
		Balancer: &kafka.LeastBytes{},
	}

	service := timewindows.CreateTimeWindowsReader(readerConfig, writerConfig, int(time.Now().UnixMilli()), 1000, 5)
	defer service.Close()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	go produce("timewindows", brokers, wg)

	service.Process()

	wg.Wait()

}

func produce(topic string, brokers []string, wg *sync.WaitGroup) {
	// create kafka writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("AAAA"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("BBBBB"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("CCCCC"),
		},
	)
	wg.Done()
	time.Sleep(2 * time.Second)

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-D"),
			Value: []byte("DDDDDD"),
		},
		kafka.Message{
			Key:   []byte("Key-E"),
			Value: []byte("EEEEEE"),
		},
		kafka.Message{
			Key:   []byte("Key-F"),
			Value: []byte("FFFFFF"),
		},
	)
	wg.Done()
	time.Sleep(2 * time.Second)

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-G"),
			Value: []byte("GGGGGG"),
		},
		kafka.Message{
			Key:   []byte("Key-H"),
			Value: []byte("HHHHHH"),
		},
		kafka.Message{
			Key:   []byte("Key-I"),
			Value: []byte("IIIIII"),
		},
	)
	wg.Done()
	time.Sleep(2 * time.Second)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

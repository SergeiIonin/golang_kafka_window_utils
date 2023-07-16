package timewindows

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestTimeWindowsKafkaService(t *testing.T) {
	brokers := []string{"localhost:9092"}

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "timewindows", 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	topicConfigs := []kafka.TopicConfig{
		kafka.TopicConfig{
			Topic:             "timewindows",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             "timewindows_aggregated",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	conn.CreateTopics(topicConfigs...)

	writerMsgsInterval := 1000 * time.Millisecond

	readerConfig := kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     "timewindows",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}

	writerConfig := kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows_aggregated",
		Balancer: &kafka.LeastBytes{},
	}

	testWriterConfig := kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows",
		Balancer: &kafka.LeastBytes{},
	}

	testReaderConfig := kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     "timewindows_aggregated",
		GroupID:   "test_group",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}

	service := CreateTimeWindowsKafkaService(readerConfig, writerConfig, int(time.Now().UnixMilli()), 250, 5)
	defer service.Close()

	// produce messages to Kafka and run service aggregating msgs in time windows
	wgProd := &sync.WaitGroup{}
	wgProd.Add(3)
	go produce(testWriterConfig, wgProd, writerMsgsInterval)
	go service.Process()
	wgProd.Wait()

	// consume messages from Kafka timewindows_aggregated
	wgCons := &sync.WaitGroup{}
	wgCons.Add(1)
	numMsgs := 3
	msgs := make([]kafka.Message, numMsgs)
	go consume(testReaderConfig, numMsgs, msgs, wgCons)
	wgCons.Wait()

	msgsStr := make([]string, numMsgs)
	for i, msg := range msgs {
		msgsStr[i] = string(msg.Value)
	}

	log.Println("received messages = \n", msgsStr)

	expected := []string{"AAA,BBB,CCC", "DDD,EEE,FFF", "GGG,HHH,III"}

	t.Run("TestTimeWindowsKafkaService", func(t *testing.T) {
		if len(msgsStr) != numMsgs {
			t.Errorf("Expected %d messages, got %d", numMsgs, len(msgs))
		}
		contains(t, msgsStr, expected)
	})
}

func produce(writerConfig kafka.WriterConfig, wg *sync.WaitGroup, writerMsgsInterval time.Duration) {
	w := kafka.NewWriter(writerConfig)

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("AAA"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("BBB"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("CCC"),
		},
	)
	wg.Done()
	time.Sleep(writerMsgsInterval)

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-D"),
			Value: []byte("DDD"),
		},
		kafka.Message{
			Key:   []byte("Key-E"),
			Value: []byte("EEE"),
		},
		kafka.Message{
			Key:   []byte("Key-F"),
			Value: []byte("FFF"),
		},
	)
	wg.Done()
	time.Sleep(writerMsgsInterval)

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-G"),
			Value: []byte("GGG"),
		},
		kafka.Message{
			Key:   []byte("Key-H"),
			Value: []byte("HHH"),
		},
		kafka.Message{
			Key:   []byte("Key-I"),
			Value: []byte("III"),
		},
	)
	wg.Done()

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func consume(readerConfig kafka.ReaderConfig, count int, msgs []kafka.Message, wg *sync.WaitGroup) {
	r := kafka.NewReader(readerConfig)
	max := len(msgs)

	for i := 0; i < max; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
		}
		msgs[i] = m
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
	wg.Done()
}

func contains(t *testing.T, msgs []string, expected []string) bool {
	if len(msgs) != len(expected) {
		return false
	}
	hashmap := make(map[string]bool)
	for _, msg := range msgs {
		hashmap[msg] = true
	}

	for _, msg := range expected {
		if _, ok := hashmap[msg]; !ok {
			t.Errorf("msg %s not found in consumed messages", msg)
			return false
		}
	}

	return true
}

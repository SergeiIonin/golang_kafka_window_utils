package timewindows

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

var TEST_ENV string
var brokers []string
var conn *kafka.Conn
var writerMsgsInterval time.Duration
var readerConfig kafka.ReaderConfig
var writerConfig kafka.WriterConfig
var testWriterConfig kafka.WriterConfig
var testReaderConfig kafka.ReaderConfig
var service TimeWindowsKafkaService

func TestTimeWindowsKafkaService(t *testing.T) {
	TEST_ENV = os.Getenv("env")
	if TEST_ENV == "docker" {
		log.Println("DOCKER ENV is used")
		pwd, _ := os.Getwd()
		dockerComposeDir := fmt.Sprintf("%s/%s", pwd, "/docker_test/docker-compose.yaml")
		identifier := tc.StackIdentifier("kafka_timewindow_test")
		kafkaCompose, err := tc.NewDockerComposeWith(tc.WithStackFiles(dockerComposeDir), identifier)

		if err != nil {
			log.Fatal("Failed to create compose: ", err)
		}

		kafkaCompose.Up(context.TODO())
		t.Cleanup(func() {
			assert.NoError(t, kafkaCompose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveImagesLocal), "compose.Down()")
		})

		err = kafkaCompose.WaitForService("kafka", wait.ForListeningPort("9092").WithStartupTimeout(30*time.Second)).Up(context.TODO(), tc.Wait(true))
		//	err = compose.WaitForService("kafka", wait.ForListeningPort("9092")).Up(context.TODO(), tc.Wait(true))

		if err != nil {
			log.Fatal("Kafka is not accessible: ", err)
		}
	}

	log.Println("Dialing Kafka...")
	brokers = []string{"127.0.0.1:9092"}

	//conn, err := dialKafka(context.Background(), "tcp", brokers[0])
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokers[0], "timewindows", 0)
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

	writerMsgsInterval = 1000 * time.Millisecond

	readerConfig = kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     "timewindows",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}
	writerConfig = kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows_aggregated",
		Balancer: &kafka.LeastBytes{},
	}
	testReaderConfig = kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     "timewindows_aggregated",
		GroupID:   "test_group",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}
	testWriterConfig = kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows",
		Balancer: &kafka.LeastBytes{},
	}

	service = CreateTimeWindowsKafkaService(readerConfig, writerConfig, int(time.Now().UnixMilli()), 250, 5)

	defer service.Close()
	// produce messages to Kafka and run service aggregating msgs in time windows
	wgProd := &sync.WaitGroup{}
	wgProd.Add(3)
	go produce(testWriterConfig, wgProd, writerMsgsInterval, t)
	go service.Process()
	wgProd.Wait()

	// consume messages from Kafka timewindows_aggregated
	wgCons := &sync.WaitGroup{}
	wgCons.Add(1)
	numMsgs := 3
	msgs := make([]kafka.Message, numMsgs)
	go consume(testReaderConfig, numMsgs, msgs, wgCons, t)
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

func produce(writerConfig kafka.WriterConfig, wg *sync.WaitGroup, writerMsgsInterval time.Duration, t *testing.T) {
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
		log.Println("failed to write messages:", err)
		t.Failed()
		//log.Fatal("failed to write messages:", err)
	}
}

func consume(readerConfig kafka.ReaderConfig, count int, msgs []kafka.Message, wg *sync.WaitGroup, t *testing.T) {
	r := kafka.NewReader(readerConfig)
	max := len(msgs)

	for i := 0; i < max; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", err)
			t.Failed()
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

package timewindows

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/SergeiIonin/golang_kafka_window_utils/testutils"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestTimeWindowsKafkaServicePartitioned(t *testing.T) {
	var TEST_ENV string
	var brokers []string
	var writerMsgsInterval time.Duration
	var readerConfig kafka.ReaderConfig
	var writerConfig kafka.WriterConfig
	var testWriterConfig kafka.WriterConfig
	var testReaderConfig kafka.ReaderConfig
	var service TimeWindowsKafkaService

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

		err = kafkaCompose.WaitForService("kafka", wait.ForListeningPort("29092").WithStartupTimeout(30*time.Second)).Up(context.TODO(), tc.Wait(true))

		if err != nil {
			log.Fatal("Kafka is not accessible: ", err)
		}
	}

	log.Println("Dialing Kafka...")
	brokerAddr := "localhost:29092"
	brokers = []string{brokerAddr}

	client := &kafka.Client{
		Addr:      kafka.TCP(brokerAddr),
		Timeout:   30 * time.Second,
		Transport: nil,
	}

	topicConfigs := []kafka.TopicConfig{
		kafka.TopicConfig{
			Topic:             "timewindows",
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             "timewindows_aggregated",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	req := &kafka.CreateTopicsRequest{
		Addr:         kafka.TCP(brokerAddr),
		Topics:       topicConfigs,
		ValidateOnly: false,
	}

	_, err := client.CreateTopics(context.Background(), req)
	if err != nil {
		log.Printf("could not create topic %v", err)
	}

	writerMsgsInterval = 1000 * time.Millisecond

	readerConfig = kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "timewindows",
		GroupID:  "service_group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}
	writerConfig = kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows_aggregated",
		Balancer: &kafka.LeastBytes{},
	}
	testReaderConfig = kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "timewindows_aggregated",
		GroupID:  "test_group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}
	testWriterConfig = kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "timewindows",
		Balancer: &kafka.LeastBytes{},
	}

	service = CreateTimeWindowsKafkaService(readerConfig, writerConfig, int(time.Now().UnixMilli()), 250, 50)

	defer service.Close()
	// produce messages to Kafka and run service aggregating msgs in time windows
	wgProd := &sync.WaitGroup{}
	wgProd.Add(3)
	go testutils.Produce(testWriterConfig, wgProd, writerMsgsInterval, t)
	go service.Process()
	wgProd.Wait()

	// consume messages from Kafka timewindows_aggregated
	wgCons := &sync.WaitGroup{}
	wgCons.Add(1)
	numMsgs := 3
	msgs := make([]kafka.Message, numMsgs)
	go testutils.Consume(testReaderConfig, numMsgs, msgs, wgCons, t)
	wgCons.Wait()

	msgsStr := make([]string, numMsgs)
	for i, msg := range msgs {
		msgsStr[i] = string(msg.Value)
	}

	log.Println("received messages = \n", msgsStr)

	expected := []string{"AAA,BBB,CCC", "DDD,EEE,FFF", "GGG,HHH,III"}
	offsets := make([]int, len(msgs))
	for i, msg := range msgs {
		offsets[i] = int(msg.Offset)
	}

	t.Run("TestTimeWindowsKafkaServicePartitioned", func(t *testing.T) {
		if len(msgsStr) != numMsgs {
			t.Errorf("Expected %d messages, got %d", numMsgs, len(msgs))
		}
		testutils.Contains(t, msgsStr, expected)
		testutils.IsSorted(offsets)
	})
}

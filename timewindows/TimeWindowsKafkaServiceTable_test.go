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

func TestTWKSTable(t *testing.T) {
	var TEST_ENV string
	var brokers []string

	var writerMsgsInterval time.Duration
	var service *TimeWindowsKafkaService

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

		kafkaCompose.Up(context.Background())
		t.Cleanup(func() {
			assert.NoError(t, kafkaCompose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveImagesLocal), "compose.Down()")
		})

		err = kafkaCompose.WaitForService("kafka", wait.ForListeningPort("29092").WithStartupTimeout(30*time.Second)).Up(context.Background(), tc.Wait(true))

		if err != nil {
			log.Fatal("Kafka is not accessible: ", err)
		}
	}

	brokerAddr := "localhost:29092"
	brokers = []string{brokerAddr}

	client := &kafka.Client{
		Addr: kafka.TCP(brokerAddr),
		//Timeout:   30 * time.Second,
		Transport: nil,
	}

	writerMsgsInterval = 1000 * time.Millisecond

	expected := []string{"AAA,BBB,CCC", "DDD,EEE,FFF", "GGG,HHH,III"}

	testDataOnePartition := 
		makeTestData("single partition test", brokers, "timewindows", "timewindows_aggregated", 1, "service_group_0", "test_group_0", expected)

	testDataMultiplePartitions := 
		makeTestData("multiple partitions test", brokers, "timewindows_1", "timewindows_aggregated_1", 3, "service_group_1", "test_group_1", expected)

	for _, testData := range []TestData{testDataOnePartition, testDataMultiplePartitions} {
		req := &kafka.CreateTopicsRequest{
			Addr:         kafka.TCP(brokerAddr),
			Topics:       testData.topicConfigs,
			ValidateOnly: false,
		}

		_, err := client.CreateTopics(context.Background(), req)
		if err != nil {
			log.Printf("could not create topic %v", err)
		}

		service = CreateTimeWindowsKafkaService(testData.readerConfig, testData.writerConfig, int(time.Now().UnixMilli()), 250, 50)

		// produce messages to Kafka and run service aggregating msgs in time windows
		wgProd := &sync.WaitGroup{}
		wgProd.Add(3)
		go testutils.Produce(testData.testWriterConfig, wgProd, writerMsgsInterval, t)
		go service.Process()
		wgProd.Wait()

		// consume messages from Kafka timewindows_aggregated
		wgCons := &sync.WaitGroup{}
		wgCons.Add(1)
		numMsgs := 3
		msgs := make([]kafka.Message, numMsgs)
		go testutils.Consume(testData.testReaderConfig, numMsgs, msgs, wgCons, t)
		wgCons.Wait()

		msgsStr := make([]string, numMsgs)
		for i, msg := range msgs {
			msgsStr[i] = string(msg.Value)
		}

		log.Println("received messages = \n", msgsStr)

		offsets := make([]int, len(msgs))
		for i, msg := range msgs {
			offsets[i] = int(msg.Offset)
		}

		t.Run(testData.name, func(t *testing.T) {
			log.Println("Running test: ", testData.name)
			if len(msgsStr) != numMsgs {
				t.Errorf("Expected %d messages, got %d", numMsgs, len(msgs))
			}
			testutils.Contains(t, msgsStr, testData.expectedMsgs)
			testutils.IsSorted(offsets)

			delReq := &kafka.DeleteTopicsRequest{
				Addr:   kafka.TCP(brokerAddr),
				Topics: []string{"timewindows", "timewindows_aggregated"},
			}
			_, err = client.DeleteTopics(context.Background(), delReq)
			if err == nil {
				log.Println("topics timewindows and timewindows_aggregated were deleted")
			}
			assert.NoError(t, err)
			assert.NoError(t, service.Close())
		})
	}
}

type TestData struct {
	name             string
	topicConfigs     []kafka.TopicConfig
	readerConfig     kafka.ReaderConfig
	writerConfig     kafka.WriterConfig
	testReaderConfig kafka.ReaderConfig
	testWriterConfig kafka.WriterConfig
	expectedMsgs     []string
}

func makeTestData(name string, brokers []string, topic string, topicAggregated string, partitionsNum int, serviceCG string, testSG string, expectedMsgs []string) TestData {
	return TestData{
		name: name,
		topicConfigs: []kafka.TopicConfig{
			kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     partitionsNum,
				ReplicationFactor: 1,
			},
			kafka.TopicConfig{
				Topic:             topicAggregated,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
		readerConfig: kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  serviceCG,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		},
		writerConfig: kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topicAggregated,
			Balancer: &kafka.LeastBytes{},
		},
		testReaderConfig: kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topicAggregated,
			GroupID:  testSG,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		},
		testWriterConfig: kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		expectedMsgs: expectedMsgs,
	}
}

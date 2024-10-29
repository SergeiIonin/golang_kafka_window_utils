package timewindows

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/SergeiIonin/golang_kafka_window_utils/testutils"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/docker/docker/client"	
)

var (
	kafkaBroker = "localhost:9092"
	kafkaAddr   = kafka.TCP(kafkaBroker)
	err         error
	containerId  string
	dockerClient *client.Client
)

func init() {
	dockerClient, err = client.NewClientWithOpts(client.WithVersion("1.45"))
	if err != nil {
		log.Fatalf("error creating docker client: %s", err.Error())
	}
	containerId, err = testutils.CreateKafkaWithKRaftContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create container %v", err)
	}

	log.Printf("Container ID: %s", containerId)
}

// fixme two test don't pass together, probably multiple partitions test is not working properly
func TestTWKSTableKraft_test(t *testing.T) {
	cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}

	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	
	t.Cleanup(cleanup)

	var service *TimeWindowsKafkaService

	brokers := []string{kafkaBroker}
	writerMsgsInterval := 1000 * time.Millisecond

	kafkaClient := &kafka.Client{
		Addr: kafkaAddr,
		//Timeout:   30 * time.Second,
		Transport: nil,
	}

	if _, err = kafkaClient.Heartbeat(context.Background(), &kafka.HeartbeatRequest{
		Addr:            kafkaAddr,
		GroupID:         "",
		GenerationID:    0,
		MemberID:        "",
		GroupInstanceID: "",
	}); err != nil {
		log.Fatal("Kafka is not accessible: ", err)
	}

	expected := []string{"AAA,BBB,CCC", "DDD,EEE,FFF", "GGG,HHH,III"}

	testDataOnePartition :=
		testutils.MakeTestData("single partition test", brokers, "timewindows_one_part", "timewindows_aggregated", 1, "service_group_one", "test_group_one", expected)

	testDataMultiplePartitions :=
		testutils.MakeTestData("multiple partitions test", brokers, "timewindows_multi_part", "timewindows_aggregated_1", 3, "service_group_multi", "test_group_multi", expected)

	allTopicConfigs	:= append(testDataOnePartition.TopicConfigs, testDataMultiplePartitions.TopicConfigs...)

	numGroupsExpected := len(expected)

	createTopics(kafkaClient, allTopicConfigs)

	for _, testData := range []testutils.TestData{testDataOnePartition, testDataMultiplePartitions} {
		t.Run(testData.Name, func(t *testing.T) {
			log.Println("Running test: ", testData.Name)

			readerConfig := testData.ReaderConfig
			writer := testData.Writer
			testWriter := testData.TestWriter
			testReaderConfig := testData.TestReaderConfig
	
			service = CreateTimeWindowsKafkaService(readerConfig, writer, int(time.Now().UnixMilli()), 250, 50)
			// run service aggregating msgs in time windows
			go service.Process()
	
			// produce messages to Kafka
			produceTestMessages(numGroupsExpected, testWriter, writerMsgsInterval, t)
			
			// consume messages from Kafka timewindows_aggregated
			msgs := make([]kafka.Message, numGroupsExpected)

			consumeMessages(msgs, testReaderConfig, t)

			msgsStr := make([]string, 0, numGroupsExpected)
			offsets := make([]int, 0, len(msgs))
			for _, msg := range msgs {
				msgsStr = append(msgsStr, string(msg.Value))
				offsets = append(offsets, int(msg.Offset))
			}
	
			log.Println("received messages = \n", msgsStr)
	
			if len(msgsStr) != numGroupsExpected {
				t.Errorf("Expected %d messages, got %d", numGroupsExpected, len(msgs))
			}

			testutils.Contains(t, msgsStr, testData.ExpectedMsgs)
			testutils.IsSorted(offsets)
			assert.NoError(t, service.Close())
		})
	}
}

func createTopics(kafkaClient *kafka.Client, topicConfigs []kafka.TopicConfig) {
	req := &kafka.CreateTopicsRequest{
		Addr:         kafkaAddr,
		Topics:       topicConfigs,
		ValidateOnly: false,
	}

	_, err := kafkaClient.CreateTopics(context.Background(), req)
	if err != nil {
		log.Printf("could not create topic %v", err)
	}
}

func produceTestMessages(numGroupsExpected int, writer *kafka.Writer, writerMsgsInterval time.Duration, t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(numGroupsExpected)
	go testutils.Produce(writer, wg, writerMsgsInterval, t)
	wg.Wait()
}

func consumeMessages(msgs []kafka.Message, testReaderConfig kafka.ReaderConfig, t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := kafka.NewReader(testReaderConfig)
	go testutils.Consume(reader, msgs, wg, t)
	wg.Wait()
}

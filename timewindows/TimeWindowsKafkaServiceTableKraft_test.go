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
)

var (
	kafkaBroker = "localhost:9092"
	kafkaAddr   = kafka.TCP(kafkaBroker)
	err         error
	/*containerId  string
	dockerClient *client.Client*/
)

/*func init() {
	dockerClient, err = client.NewClientWithOpts(client.WithVersion("1.45"))
	if err != nil {
		log.Printf("error creating docker client: %s", err.Error())
		panic(err)
	}
	containerId, err = testutils.CreateKafkaWithKRaftContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create container %v", err)
	}

	log.Printf("Container ID: %s", containerId)
}*/

func TestTWKSTableKraft_test(t *testing.T) {
	/*cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}

	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)*/

	var brokers []string

	var writerMsgsInterval time.Duration
	var service *TimeWindowsKafkaService

	brokers = []string{kafkaBroker}

	kafkaClient := &kafka.Client{
		Addr: kafkaAddr,
		//Timeout:   30 * time.Second,
		Transport: nil,
	}

	_, err = kafkaClient.Heartbeat(context.Background(), &kafka.HeartbeatRequest{
		Addr:            kafkaAddr,
		GroupID:         "",
		GenerationID:    0,
		MemberID:        "",
		GroupInstanceID: "",
	})

	if err != nil {
		log.Fatal("Kafka is not accessible: ", err)
	}

	writerMsgsInterval = 1000 * time.Millisecond

	expected := []string{"AAA,BBB,CCC", "DDD,EEE,FFF", "GGG,HHH,III"}

	testDataOnePartition :=
		testutils.MakeTestData("single partition test", brokers, "timewindows", "timewindows_aggregated", 1, "service_group_0", "test_group_0", expected)

	testDataMultiplePartitions :=
		testutils.MakeTestData("multiple partitions test", brokers, "timewindows_1", "timewindows_aggregated_1", 3, "service_group_1", "test_group_1", expected)

	for _, testData := range []testutils.TestData{testDataOnePartition, testDataMultiplePartitions} {
		req := &kafka.CreateTopicsRequest{
			Addr:         kafkaAddr,
			Topics:       testData.TopicConfigs,
			ValidateOnly: false,
		}

		_, err := kafkaClient.CreateTopics(context.Background(), req)
		if err != nil {
			log.Printf("could not create topic %v", err)
		}

		service = CreateTimeWindowsKafkaService(testData.ReaderConfig, testData.Writer, int(time.Now().UnixMilli()), 250, 50)

		// produce messages to Kafka and run service aggregating msgs in time windows
		wgProd := &sync.WaitGroup{}
		wgProd.Add(3)
		go testutils.Produce(testData.TestWriter, wgProd, writerMsgsInterval, t)
		go service.Process()
		wgProd.Wait()

		// consume messages from Kafka timewindows_aggregated
		wgCons := &sync.WaitGroup{}
		wgCons.Add(1)
		numMsgs := 3
		msgs := make([]kafka.Message, numMsgs)
		reader := kafka.NewReader(testData.TestReaderConfig)
		go testutils.Consume(reader, numMsgs, msgs, wgCons, t)
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

		t.Run(testData.Name, func(t *testing.T) {
			log.Println("Running test: ", testData.Name)
			if len(msgsStr) != numMsgs {
				t.Errorf("Expected %d messages, got %d", numMsgs, len(msgs))
			}

			testutils.Contains(t, msgsStr, testData.ExpectedMsgs)
			testutils.IsSorted(offsets)
			assert.NoError(t, service.Close())
		})
	}
}

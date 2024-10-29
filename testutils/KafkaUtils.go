package testutils

import (
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"strings"
)

var (
	kafkaBroker       = "localhost:9092"
	kafkaAddr         = kafka.TCP(kafkaBroker)
	topics            = []string{"test_1", "test_2", "test_3"}
	mergedSourceTopic = "test_merged"
	msgsPerTopic      = 15
	numTopics         = len(topics)
	kafka_client      *kafka.Client
	containerId       string
	err               error
	dockerClient      *client.Client
)

// This is not an actual offsets from the merged data topic, but the offsets from the original topics, which is
// presented in each message's value as a suffix, e.g. value-test_1-3, where test_1 is the original topic and 3 is the offset
func GetOffsetsPerTopic(messages []kafka.Message) map[string][]int64 {
	offsetsPerTopic := make(map[string][]int64)
	getOffset := func(value string) int64 {
		elems := strings.Split(value, "-")
		offset, _ := strconv.ParseInt(elems[len(elems)-1], 10, 64)
		return offset
	}
	var header string
	var offset int64
	for _, m := range messages {
		header = string(m.Headers[0].Value)
		if _, ok := offsetsPerTopic[header]; !ok {
			offsetsPerTopic[header] = make([]int64, 0)
		}
		offset = getOffset(string(m.Value))
		offsetsPerTopic[header] = append(offsetsPerTopic[header], offset)
	}
	log.Printf("Offsets per topic: %v", offsetsPerTopic)
	return offsetsPerTopic
}

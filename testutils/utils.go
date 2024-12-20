package testutils

import (
	"context"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func MakeTestData(name string, brokers []string, topic string, topicAggregated string, partitionsNum int, serviceCG string, testSG string, expectedMsgs []string) TestData {
	return TestData{
		Name: name,
		TopicConfigs: []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     partitionsNum,
				ReplicationFactor: 1,
			},
			{
				Topic:             topicAggregated,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
		ReaderConfig: kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        serviceCG,
			MinBytes:       1,    // 1B
			MaxBytes:       10e6, // 10MB
			ReadBackoffMin: 5 * time.Millisecond,
			ReadBackoffMax: 10 * time.Millisecond,
			StartOffset:    kafka.FirstOffset,
		},
		Writer: &kafka.Writer{
			Addr:            kafka.TCP(brokers[0]),
			Topic:           topicAggregated,
			WriteBackoffMin: 1 * time.Millisecond,
			WriteBackoffMax: 5 * time.Millisecond,
			BatchTimeout:    1 * time.Millisecond,
			Balancer:        &kafka.RoundRobin{},
		},
		TestWriter: &kafka.Writer{
			Addr:            kafka.TCP(brokers[0]),
			Topic:           topic,
			WriteBackoffMin: 1 * time.Millisecond,
			WriteBackoffMax: 5 * time.Millisecond,
			BatchTimeout:    1 * time.Millisecond,
			Balancer:        &kafka.RoundRobin{},
		},
		TestReaderConfig: kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topicAggregated,
			GroupID:        testSG,
			MinBytes:       1,    // 1B
			MaxBytes:       10e6, // 10MB
			ReadBackoffMin: 5 * time.Millisecond,
			ReadBackoffMax: 10 * time.Millisecond,
			StartOffset:    kafka.FirstOffset,
		},
		ExpectedMsgs: expectedMsgs,
	}
}

func Produce(writer *kafka.Writer, wg *sync.WaitGroup, writerMsgsInterval time.Duration, t *testing.T) {
	t0 := time.Now()

	log.Printf("[producer] starting to write messages at %s\n", t0)
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("AAA"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("BBB"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("CCC"),
			Time:  t0,
		},
	)
	if err != nil {
		log.Println("[producer] failed to write messages:", err)
		t.Failed()
	}
	wg.Done()

	t0 = t0.Add(writerMsgsInterval)

	log.Printf("[producer] starting to write messages at %s\n", t0)
	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-D"),
			Value: []byte("DDD"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-E"),
			Value: []byte("EEE"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-F"),
			Value: []byte("FFF"),
			Time:  t0,
		},
	)
	if err != nil {
		log.Println("[producer] failed to write messages:", err)
		t.Failed()
	}
	wg.Done()

	t0 = t0.Add(writerMsgsInterval)

	log.Printf("[producer] starting to write messages at %s\n", t0)
	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-G"),
			Value: []byte("GGG"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-H"),
			Value: []byte("HHH"),
			Time:  t0,
		},
		kafka.Message{
			Key:   []byte("Key-I"),
			Value: []byte("III"),
			Time:  t0,
		},
	)
	wg.Done()

	if err != nil {
		log.Println("[producer] failed to write messages:", err)
		t.Failed()
	}
	err = writer.Close()
	if err != nil {
		log.Println("[producer] failed to close writer:", err)
		t.Failed()
	}

	defer func() {
		err := writer.Close()
		if err != nil {
			log.Println("[producer] failed to close writer:", err)
			t.Failed()
		}
	}()
}

// fixme either consume 3 messages or exit due to timeout
func Consume(reader *kafka.Reader, msgs []kafka.Message, wg *sync.WaitGroup, t *testing.T) {
	max := len(msgs)

	for i := 0; i < max; i++ {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", err)
			t.Failed()
		}
		msgs[i] = m
		log.Printf("[consumer] message from partition %d, offset %d: %s = %s\n", m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
	wg.Done()
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Println("[consumer] failed to close reader:", err)
			t.Failed()
		}
	}()
}

// Checks if teh messages from expected array all exist in the msgs
func Equals(t *testing.T, msgs []string, expected []string) bool {
	if len(msgs) != len(expected) {
		return false
	}
	hashmap := make(map[string]bool)
	for _, msg := range msgs {
		hashmap[msg] = true
	}

	for _, msg := range expected {
		if _, ok := hashmap[msg]; !ok {
			t.Errorf("[contains check] msg %s not found in consumed messages", msg)
			return false
		}
	}

	return true
}

// todo simplify
func Contains(t *testing.T, msgs []string, expected []string) bool {
	if len(msgs) != len(expected) {
		return false
	}
	msgsRunes := make([][]rune, len(msgs))
	for i, msg := range msgs {
		msgsRunes[i] = []rune(msg)
	}
	expectedRunes := make([][]rune, len(expected))
	for i, msg := range expected {
		expectedRunes[i] = []rune(msg)
	}

	msgsMaps := make([]map[rune]int, len(msgs))
	for i, msg := range msgsRunes {
		msgsMaps[i] = make(map[rune]int)
		for _, r := range msg {
			msgsMaps[i][r]++
		}
	}

	expectedMaps := make([]map[rune]int, len(expected))
	for i, msg := range expectedRunes {
		expectedMaps[i] = make(map[rune]int)
		for _, r := range msg {
			expectedMaps[i][r]++
		}
	}

	for _, msg := range expectedMaps {
		found := false
		for _, msg2 := range msgsMaps {
			if equals(msg, msg2) {
				found = true
				break
			}
		}
		if !found {
			t.Error("[contains check] some msgs are not found in consumed messages", msg)
			return false
		}
	}

	return true
}

func equals(m1 map[rune]int, m2 map[rune]int) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if m2[k] != v {
			return false
		}
	}
	return true
}

func IsSorted(offsets []int) bool {
	return slices.IsSorted(offsets)
}

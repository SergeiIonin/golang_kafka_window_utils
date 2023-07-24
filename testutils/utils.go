package testutils

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func Produce(writerConfig kafka.WriterConfig, wg *sync.WaitGroup, writerMsgsInterval time.Duration, t *testing.T) {
	w := kafka.NewWriter(writerConfig)

	t0 := time.Now()

	err := w.WriteMessages(context.Background(),
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
	wg.Done()

	t0 = t0.Add(writerMsgsInterval)

	err = w.WriteMessages(context.Background(),
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
	wg.Done()

	t0 = t0.Add(writerMsgsInterval)

	err = w.WriteMessages(context.Background(),
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
		//log.Fatal("failed to write messages:", err)
	}
}

func Consume(readerConfig kafka.ReaderConfig, count int, msgs []kafka.Message, wg *sync.WaitGroup, t *testing.T) {
	r := kafka.NewReader(readerConfig)
	max := len(msgs)

	for i := 0; i < max; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", err)
			t.Failed()
		}
		msgs[i] = m
		log.Printf("[consumer] message from partition %d, offset %d: %s = %s\n", m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
	wg.Done()
}

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
	if len(offsets) == 0 {
		return true
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			return false
		}
	}
	return true
}

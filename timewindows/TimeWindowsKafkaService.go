package timewindows

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// This implementation just writes batches to other kafka topic. Messages within a batch are all produced as a single message under the same key.
// This key is the windowId of the type startMillis + n x timeWindowSizeMillis.
type TimeWindowsKafkaService struct {
	kafkaReader *kafka.Reader
	kafkaWriter *kafka.Writer
	batchBuffer *BatchBuffer
	msgChan     chan kafka.Message
}

func (s *TimeWindowsKafkaService) onBatchClear(windowId int, batch []kafka.Message) {
	s.kafkaWriter.WriteMessages(context.Background(), generateMsgFromBatch(windowId, batch))
}

func (s *TimeWindowsKafkaService) Process() {

	mutex := &sync.Mutex{}

	go s.Read(s.msgChan)

	base := 2500
	timeoutDur := time.Duration(base) * time.Millisecond // todo make configurable bc it depends on timeWindowSizeMillis

	for {
		select {
		case msg := <-s.msgChan:
			s.batchBuffer.AddToBatch(msg, s.onBatchClear)
			// time duration of N time windows
		case <-time.After(timeoutDur):
			mutex.Lock()
			s.batchBuffer.ClearBuffer(s.onBatchClear, true)
			mutex.Unlock()
		}
	}
}

// todo add error channel (?)
func (s *TimeWindowsKafkaService) Read(ch chan kafka.Message) {
	for {
		msg, err := s.kafkaReader.ReadMessage(context.Background())
		// check if the error is EOF using errors.Is
		if errors.Is(err, io.EOF) {
			log.Printf("[TWKS] reached EOF, reader has been closed")
			break
		}

		ch <- msg
	}
}

func (s *TimeWindowsKafkaService) Close() error {
	err := s.kafkaReader.Close()
	if err != nil {
		log.Printf("[TWKS] error occurred while closing kafka reader: %v", err)
		return err
	}
	err = s.kafkaWriter.Close()
	if err != nil {
		log.Printf("[TWKS] error occurred while closing kafka writer: %v", err)
		return err

	}
	return nil
}

func generateMsgFromBatch(windowId int, batch []kafka.Message) kafka.Message {
	key := []byte(fmt.Sprintf("%d", windowId))
	if len(batch) == 0 {
		return kafka.Message{Key: key, Value: []byte("")}
	} else {
		value := make([]byte, 0)
		for i, msg := range batch {
			if i > 0 {
				value = append(value, ',')
			}
			value = append(value, msg.Value...)
		}
		return kafka.Message{Key: key, Value: value}
	}
}

// todo maybe we should return pointer to TimeWindowsKafkaService
func CreateTimeWindowsKafkaService(readerConfig kafka.ReaderConfig, writerConfig kafka.WriterConfig,
	startTimeMillis int, timeWindowSizeMillis int, capacity int) *TimeWindowsKafkaService {
	reader := kafka.NewReader(readerConfig)
	writer := kafka.NewWriter(writerConfig)
	batchBuffer := CreateBatchBuffer(startTimeMillis, timeWindowSizeMillis, capacity)
	msgChan := make(chan kafka.Message)
	return &TimeWindowsKafkaService{reader, writer, batchBuffer, msgChan}
}

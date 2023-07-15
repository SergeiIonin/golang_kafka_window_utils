package timewindows

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// This implementation just writes batches to other kafka. Messages within a batch are all produced as a single message under the same key.
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

	go s.Read(s.msgChan)

	for {
		select {
		case msg := <-s.msgChan:
			log.Println("received", msg)
			s.batchBuffer.AddToBatch(msg, s.onBatchClear)
			// time duration of 5 time windows
		case <-time.After(time.Duration(5 * s.batchBuffer.timeWindowSizeMillis)):
			s.batchBuffer.ClearBuffer(s.onBatchClear, true)
		}
	}
}

// todo add error channel (?)
func (s *TimeWindowsKafkaService) Read(ch chan kafka.Message) {
	for {
		msg, err := s.kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error occurred while reading message: %v", err)
		}
		ch <- msg
	}
}

func (s *TimeWindowsKafkaService) Close() {
	s.kafkaReader.Close()
	s.kafkaWriter.Close()
}

func generateMsgFromBatch(windowId int, batch []kafka.Message) kafka.Message {
	key := []byte(fmt.Sprintf("%d", windowId))
	if len(batch) == 0 {
		return kafka.Message{Key: key, Value: []byte("")}
	} else {
		value := make([]byte, 0)
		for _, msg := range batch {
			value = append(value, ',')
			value = append(value, msg.Value...)
		}
		return kafka.Message{Key: key, Value: value}
	}
}

func CreateTimeWindowsReader(readerConfig kafka.ReaderConfig, writerConfig kafka.WriterConfig,
	startTimeMillis int, timeWindowSizeMillis int, capacity int) TimeWindowsKafkaService {
	reader := kafka.NewReader(readerConfig)
	writer := kafka.NewWriter(writerConfig)
	batchBuffer := CreateBatchBuffer(startTimeMillis, timeWindowSizeMillis, capacity)
	msgChan := make(chan kafka.Message)
	return TimeWindowsKafkaService{reader, writer, batchBuffer, msgChan}
}

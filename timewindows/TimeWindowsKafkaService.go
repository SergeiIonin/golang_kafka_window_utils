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

// A service which composes a new Kafka message out of the batch of messages collected within the time window.
// Next, new message is written to the special topic.
// New massage key is the windowId of the type startMillis + n x timeWindowSizeMillis.
type TimeWindowsKafkaService struct {
	kafkaReader *kafka.Reader
	kafkaWriter *kafka.Writer
	batchBuffer *BatchBuffer
	msgChan     chan kafka.Message
}

// Create a new TimeWindowsKafkaService with the given Kafka reader and writer, start time in milliseconds, time window size in milliseconds and capacity.
func CreateTimeWindowsKafkaService(readerConfig kafka.ReaderConfig, writer *kafka.Writer,
	startTimeMillis int, timeWindowSizeMillis int, capacity int) *TimeWindowsKafkaService {
	reader := kafka.NewReader(readerConfig)
	batchBuffer := CreateBatchBuffer(startTimeMillis, timeWindowSizeMillis, capacity)
	msgChan := make(chan kafka.Message)
	return &TimeWindowsKafkaService{reader, writer, batchBuffer, msgChan}
}

func (s *TimeWindowsKafkaService) onBatchClear(windowId int, batch []kafka.Message) (err error) {
	return s.kafkaWriter.WriteMessages(context.Background(), generateMsgFromBatch(windowId, batch))
}

// Runs the service which reads messages from Kafka and aggregates them in time windows.
func (s *TimeWindowsKafkaService) Run() {
	mutex := &sync.Mutex{}
	var err error

	go s.read(s.msgChan)

	base := 2500
	timerDuration := time.Duration(base) * time.Millisecond // todo make configurable bc it depends on timeWindowSizeMillis
	timer := time.NewTimer(timerDuration)

	for {
		select {
		case msg := <-s.msgChan:
			if err = s.batchBuffer.AddToBatch(msg, s.onBatchClear); err != nil {
				log.Fatalf("[TWKS] error occurred while adding message to batch buffer: %v", err) // we need exit here because the app is not functional anymore
			}
		case <-timer.C:
			mutex.Lock()
			if err = s.batchBuffer.ClearBuffer(s.onBatchClear, true); err != nil {
				log.Fatalf("[TWKS] error occurred while clearing batch buffer: %v", err) // we need exit here because the app is not functional
			}
			timer.Reset(timerDuration)
			mutex.Unlock()
		}
	}
}

// Closes the underlying Kafka reader and writer.
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

func (s *TimeWindowsKafkaService) read(ch chan kafka.Message) { // todo add error channel (?)
	for {
		msg, err := s.kafkaReader.ReadMessage(context.Background())
		if errors.Is(err, io.EOF) {
			log.Printf("[TWKS] reached EOF, reader has been closed")
			break
		}

		ch <- msg
	}
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

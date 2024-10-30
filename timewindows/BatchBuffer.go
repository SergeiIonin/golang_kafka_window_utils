package timewindows

import (
	"log"

	"github.com/segmentio/kafka-go"
)

// Buffer holding batches of messages, each key is one of a discrete windowId (startMillis + n x timeWindowSizeMillis)
// and the value is a batch of messages received within that window.
// Capacity is the number of batches to hold in memory. If the buffer is full, all records except the last are discarded
type BatchBuffer struct {
	underlying           map[int][]kafka.Message
	startMillis          int
	timeWindowSizeMillis int
	capacity             int
}

// Creates a new BatchBuffer with the given parameters
func CreateBatchBuffer(startMillis int, timeWindowSizeMillis int, capacity int) *BatchBuffer {
	underlying := make(map[int][]kafka.Message, capacity)
	return &BatchBuffer{underlying, startMillis, timeWindowSizeMillis, capacity}
}

// Adds a message to the buffer, additionally clearing the buffer if it is full.
func (bb *BatchBuffer) AddToBatch(msg kafka.Message, onBatchClear func(int, []kafka.Message) error) (err error) {
	if len(bb.underlying) >= bb.capacity {
		if err = bb.ClearBuffer(onBatchClear, false); err != nil {
			return err
		}
	}
	windowId := getWindowId(bb.startMillis, int(msg.Time.UnixMilli()), bb.timeWindowSizeMillis)
	batch := bb.underlying[windowId]
	bb.underlying[windowId] = append(batch, msg)
	log.Printf("[BatchBuffer] adding kafka msg %s to buffer for key %d; number of msgs for %d is %d, size of buffer is %d", string(msg.Value), windowId, windowId, len(bb.underlying[windowId]), len(bb.underlying))
	return nil
}

// Either removes all batches on timeout which happend when we haven't been receiving any new messages long enough and hence should clear the buffer.
// or removes all batches except the one corresponding to the latest windowId. onBatchClear is performed on each batch in both cases.
func (bb *BatchBuffer) ClearBuffer(onBatchClear func(int, []kafka.Message) error, isTimeout bool) (err error) {
	log.Printf("[BatchBuffer] Clearing the buffer... buffer size is %d, timeout is %t", len(bb.underlying), isTimeout)
	if len(bb.underlying) > 0 {
		if !isTimeout {
			lastKey := getLastKey(bb.underlying)
			//lastKey := ascendingKeys[lastIndex]
			lastBatch := bb.underlying[lastKey]
			for windowId, batch := range bb.underlying {
				if err = onBatchClear(windowId, batch); err != nil {
					return err
				}
			}
			clear(bb.underlying)
			bb.underlying[lastKey] = lastBatch
		} else {
			for windowId, batch := range bb.underlying {
				if err = onBatchClear(windowId, batch); err != nil {
					return err
				}
			}
			clear(bb.underlying)
		}
	}
	return nil
}

func getWindowId(start int, timestamp int, timeWindowSizeMillis int) int {
	diff := timestamp - start
	windowId := start + (diff/timeWindowSizeMillis)*timeWindowSizeMillis
	return windowId
}

func getLastKey(batches map[int][]kafka.Message) int {
	max := 0
	for key := range batches {
		if key > max {
			max = key
		}
	}
	return max
}

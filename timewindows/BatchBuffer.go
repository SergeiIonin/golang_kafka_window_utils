package timewindows

import (
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

func CreateBatchBuffer(startMillis int, timeWindowSizeMillis int, capacity int) *BatchBuffer {
	underlying := make(map[int][]kafka.Message, capacity)
	return &BatchBuffer{underlying, startMillis, timeWindowSizeMillis, capacity}
}

func (bb *BatchBuffer) GetWindowId(start int, timestamp int, timeWindowSizeMillis int) int {
	diff := timestamp - start
	windowId := start + (diff/timeWindowSizeMillis)*timeWindowSizeMillis
	return windowId
}

func (bb *BatchBuffer) AddToBatch(msg kafka.Message, onBatchClear func(int, []kafka.Message)) {
	if len(bb.underlying) >= bb.capacity {
		bb.ClearBuffer(onBatchClear, false)
	}
	windowId := bb.GetWindowId(bb.startMillis, int(msg.Time.UnixMilli()), bb.timeWindowSizeMillis)
	bb.underlying[windowId] = append(bb.underlying[windowId], msg)
}

// either removes all batches on timeout (it means that we are not receiving any new messages long enough and we should clear the buffer)
// or removes all batches except those corresponding to the latest windowId. onBatchClear is performed on each batch in both cases
func (bb *BatchBuffer) ClearBuffer(onBatchClear func(int, []kafka.Message), isTimeout bool) {
	if isTimeout {
		for windowId := range bb.underlying {
			onBatchClear(windowId, bb.underlying[windowId])
			delete(bb.underlying, windowId)
		}
	} else {
		max := bb.startMillis
		for windowId := range bb.underlying {
			if windowId < max {
				onBatchClear(windowId, bb.underlying[windowId])
				delete(bb.underlying, windowId)
			} else {
				max = windowId
			}
		}
	}
}

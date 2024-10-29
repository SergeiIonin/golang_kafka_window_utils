package timewindows

import (
	"log"
	"sort"

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
	log.Printf("[BatchBuffer] adding kafka msg %s to buffer for key %d; number of msgs for %d is %d, size of buffer is %d", string(msg.Value), windowId, windowId, len(bb.underlying[windowId]), len(bb.underlying))
}

// either removes all batches on timeout (it means that we are not receiving any new messages long enough and we should clear the buffer)
// or removes all batches except those corresponding to the latest windowId. onBatchClear is performed on each batch in both cases
func (bb *BatchBuffer) ClearBuffer(onBatchClear func(int, []kafka.Message), isTimeout bool) {
	log.Printf("[BatchBuffer] Clearing the buffer... buffer size is %d, timeout is %t", len(bb.underlying), isTimeout)
	ascendingKeys := sortKeys(bb.underlying)
	if !isTimeout && len(bb.underlying) > 0 {
		ascendingKeys = ascendingKeys[:len(ascendingKeys)-1]
	}
	for _, windowId := range ascendingKeys {
		onBatchClear(windowId, bb.underlying[windowId])
		delete(bb.underlying, windowId)
	}
}

func sortKeys(batches map[int][]kafka.Message) []int {
	keys := make([]int, 0, len(batches))
	for k := range batches {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

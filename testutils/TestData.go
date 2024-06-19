package testutils

import "github.com/segmentio/kafka-go"

type TestData struct {
	Name             string
	TopicConfigs     []kafka.TopicConfig
	ReaderConfig     kafka.ReaderConfig
	Writer           *kafka.Writer
	TestWriter       *kafka.Writer
	TestReaderConfig kafka.ReaderConfig
	//testWriterConfig kafka.WriterConfig
	ExpectedMsgs []string
}

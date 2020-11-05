package kafka


import (
	"fmt"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	//"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/go-kit/kit/log"
	"gopkg.in/yaml.v2"
)

const (
	//bootstrapServers = "localhost:9092"
)

type KafkaConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers"`
	Topic string `yaml:"topic"`
}

// Break out into a separate package
type EventMessage struct {
	ID string `json:"id"`
	Type string `json:"type"`
	Data json.RawMessage `json:"data"`
}

type EventData struct {
	Value string
}

type DataframeProducer interface {
	Send(df dataframe.Dataframe) error
}

type DataframeKafkaProducer struct {
	logger log.Logger
	producer kafka.Producer
	topic string
}

func (p DataframeKafkaProducer) Close() {
	p.producer.Close()
}

// TODO Break this out into some sort of Message generator so that it can be defined caller.
func (p DataframeKafkaProducer) createMessageData(row dataframe.Row, schema dataframe.Schema)  ([]byte, error) {
	// TODO Use the values from the actual dataframe.
	data, err := json.Marshal(EventData{"TEST"})
	if err != nil {
		return nil, err
	}
	rawData := json.RawMessage(data)
	m := EventMessage{"1234", "metric-read", rawData}
	value, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (p DataframeKafkaProducer) Send(df dataframe.Dataframe) (int, error) {
	count := 0
	iter := df.RowsIterator()
	for iter.Next() {
		data, err := p.createMessageData(iter.At(), df.Schema())
		if err != nil {
			return 0, err
		}

		perr := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topic,
				Partition: kafka.PartitionAny},
			Value: data}, nil)
		if perr != nil {
			return 0, perr
		}

		e := <-p.producer.Events()
		switch e.(type) {
		case *kafka.Message:
			message := e.(*kafka.Message)
			if message.TopicPartition.Error != nil {
				// TODO Return error here.
				fmt.Printf("failed to deliver message: %v\n",
					message.TopicPartition)
			} else {
				fmt.Printf("delivered to topic %s [%d] at offset %v\n",
					*message.TopicPartition.Topic,
					message.TopicPartition.Partition,
					message.TopicPartition.Offset)
				count++
			}

		case kafka.Error:
			fmt.Printf("Unable to send message: %v",e)
		}
	}
	return count, nil
}

func NewDataframeKafkaProducer(logger log.Logger, confYaml []byte) (*DataframeKafkaProducer, error) {
	cfg := KafkaConfig{}
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, err
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})

	if err != nil {
		return nil, err
	}
	return &DataframeKafkaProducer{logger: logger, producer: *producer, topic: cfg.Topic}, nil
}

//func main() {
//	//topic := "go-test-topic"
//	//producer, err := kafka.NewProducer(&kafka.ConfigMap{
//	//	"bootstrap.servers": bootstrapServers,
//	//})
//
//	//if err != nil {
//	//	fmt.Println("Failed to create producer: %s", err)
//	//}
//
//	//data, err := json.Marshal(EventData{"TEST"})
//	//if err != nil {
//	//	fmt.Println("Error: %s", err)
//	//}
//	//rawData := json.RawMessage(data)
//	//m := EventMessage{"1234", "metric-read", rawData}
//	//value, err := json.Marshal(m)
//	//if err != nil {
//	//	fmt.Println("Error2: %s", err)
//	//}
//	//
//	//perr := producer.Produce(&kafka.Message{
//	//	TopicPartition: kafka.TopicPartition{Topic: &topic,
//	//		Partition: kafka.PartitionAny},
//	//	Value: value}, nil)
//	//
//	//if perr != nil {
//	//	fmt.Println("ERROR: %s", perr)
//	//}
//	//
//	//e := <-producer.Events()
//	//
//	//switch e.(type) {
//	//case *kafka.Message:
//	//	message := e.(*kafka.Message)
//	//	if message.TopicPartition.Error != nil {
//	//		fmt.Printf("failed to deliver message: %v\n",
//	//			message.TopicPartition)
//	//	} else {
//	//		fmt.Printf("delivered to topic %s [%d] at offset %v\n",
//	//			*message.TopicPartition.Topic,
//	//			message.TopicPartition.Partition,
//	//			message.TopicPartition.Offset)
//	//	}
//	//
//	//case kafka.Error:
//	//	fmt.Printf("CAUGHT ERROR: %v",e)
//	//}
//
//	//
//	//
//	//producer.Close()
//
//}


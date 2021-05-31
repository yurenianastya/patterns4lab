package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type criminalCase struct {
	Id          string `json:"id"`
	CaseNumber  string `json:"case_number"`
	Block       string `json:"block"`
	PrimaryType string `json:"primary_type"`
}

func parseJson() []byte {
	url := "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

	caseClient := http.Client{
		Timeout: time.Second * 20,
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Case-Agent", "criminal-case")
	res, getErr := caseClient.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}
	if res.Body != nil {
		defer res.Body.Close()
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}
	return body
}

func GetEnvVariable(key string) string {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error while reading config file %s", err)
	}
	value, ok := viper.Get(key).(string)
	if !ok {
		log.Fatalf("Invalid type assertion")
	}
	return value
}

func connectProducer(brokersURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func pushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"192.168.0.103:9092"}
	producer, err := connectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(producer)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic, partition, offset)
	return nil
}

func connectConsumer(brokerURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// NewConsumer creates a new consumer using the given broker addresses and configuration
	conn, err := sarama.NewConsumer(brokerURL, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func consumerReceiver() {
	topic := "test"
	worker, err := connectConsumer([]string{"192.168.0.103:9092"})
	if err != nil {
		panic(err)
	}
	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n",
					msgCount, msg.Topic, string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func main() {
	criminalCases := parseJson()
	switch GetEnvVariable("MODE") {
	case "console":
		var case1 []criminalCase
		jsonErr := json.Unmarshal(criminalCases, &case1)
		if jsonErr != nil {
			log.Fatal(jsonErr)
		}
		fmt.Println(case1)
	case "kafka":
		err := pushCommentToQueue("test", parseJson())
		if err != nil {
			log.Fatal(err)
		}
		consumerReceiver()
	}
}

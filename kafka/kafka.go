package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var (
	client sarama.SyncProducer
	msgKfk chan *sarama.ProducerMessage
)

func Init(address []string, chanSize int64) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Errorf("producer closed err:%v", err)
		return
	}

	//初始化
	msgKfk=make(chan *sarama.ProducerMessage,chanSize)
	go sendKafkaMsg()

	return
}
func sendKafkaMsg()  {
	for{
		select {
			case msg:= <-msgKfk:
				// 发送消息
				pid, offset, err := client.SendMessage(msg)
				if err != nil {
					log.Errorf("send msg failed, err:%v", err)
					return
				}
				log.Infof("send msg to kafka success , pid:%v offset:%v\n", pid, offset)
		}
	}
}
func SendChanMsg(msg *sarama.ProducerMessage)  {
	msgKfk <- msg
}

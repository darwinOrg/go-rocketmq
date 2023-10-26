package dgrocket

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"time"
)

type PushConsumerClient struct {
	pushConsumer rocketmq.PushConsumer
}

type SubscribeHandler func(body string) error

func NewPushConsumerClient(servers []string, group string) (*PushConsumerClient, error) {
	pushConsumer, err := rocketmq.NewPushConsumer(consumer.WithNameServer(servers), consumer.WithGroupName(group))
	if err != nil {
		return nil, err
	}

	return &PushConsumerClient{pushConsumer: pushConsumer}, nil
}

func (c *PushConsumerClient) Start() {
	if err := c.pushConsumer.Start(); err != nil {
		panic("启动consumer失败")
	}
}

func (c *PushConsumerClient) Shutdown() {
	err := c.pushConsumer.Shutdown()
	if err != nil {
		panic("关闭consumer失败")
	}
}

func (c *PushConsumerClient) Subscribe(topic string, handler SubscribeHandler) error {
	return c.pushConsumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			body := string(msg.Body)
			nowStr := time.Now().Format("2006-01-02 15:04:05")
			fmt.Printf("%s 读取到一条消息，topic: %s, 消息id: %s, 消息内容: %s \n", nowStr, msg.Topic, msg.MsgId, body)
			err := handler(body)
			if err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	})
}

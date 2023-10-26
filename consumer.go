package dgrocket

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/google/uuid"
)

type PushConsumerClient struct {
	pushConsumer rocketmq.PushConsumer
}

type SubscribeHandler func(ctx *dgctx.DgContext, body string) error

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
	return c.pushConsumer.Subscribe(topic, consumer.MessageSelector{}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			body := string(msg.Body)
			ctx := &dgctx.DgContext{TraceId: uuid.NewString() + "_rocketmq"}
			dglogger.Infof(ctx, "读取到一条消息，topic: %s, 消息id: %s, 消息内容: %s", msg.Topic, msg.MsgId, body)
			err := handler(ctx, body)
			if err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	})
}

package dgrocket

import (
	"context"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
)

type ProducerClient struct {
	producer rocketmq.Producer
}

func NewProducerClient(servers []string, retries int) (*ProducerClient, error) {
	newProducer, err := rocketmq.NewProducer(producer.WithNameServer(servers), producer.WithRetry(retries))
	if err != nil {
		return nil, err
	}

	return &ProducerClient{producer: newProducer}, nil
}

func (c *ProducerClient) Start() {
	if err := c.producer.Start(); err != nil {
		panic("启动producer失败")
	}
}

func (c *ProducerClient) Shutdown() {
	err := c.producer.Shutdown()
	if err != nil {
		panic("关闭producer失败")
	}
}

func (c *ProducerClient) SendSync(ctx *dgctx.DgContext, topic string, obj any) error {
	content, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	res, err := c.producer.SendSync(context.Background(), primitive.NewMessage(topic, content))
	if err != nil {
		dglogger.Infof(ctx, "消息发送失败: %v", err)
		return err
	}

	dglogger.Infof(ctx, "消息发送成功: %s", res.String())
	return nil
}

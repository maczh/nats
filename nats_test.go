package nats

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNats(t *testing.T) {
	str := `go:
  data:
    nats:
      uri: nats://192.168.2.20:4222
      user: macro
      password: MacZh20070804
`
	NATS.Init([]byte(str))
	conn, err := NATS.GetConnection()
	if err != nil {
		t.Fatal(err)
	}
	if !conn.Conn.IsConnected() {
		t.Fatal("Nats server not connected")
	}
	err = NATS.SubscribeQueue("", "jh.>", "jh.queue", func(msg *nats.Msg) {
		fmt.Printf("消费者1收到主题%s的消息:%s\n", msg.Subject, string(msg.Data))
	})
	if err != nil {
		t.Fatal(err)
	}
	err = NATS.SubscribeQueue("", "jh.>", "jh.queue", func(msg *nats.Msg) {
		fmt.Printf("消费者2收到主题%s的消息:%s\n", msg.Subject, string(msg.Data))
	})
	if err != nil {
		t.Fatal(err)
	}
	// conn.Conn.Flush()
	for i := 1; i <= 10; i++ {
		NATS.Publish("", "jh.test.order", []byte(fmt.Sprintf("测试消息: %02d", i)))
	}
	time.Sleep(2 * time.Second) // 等待消息处理
	NATS.Close()
}

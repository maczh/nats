package nats

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/nats-io/nats.go"
	"github.com/sadlil/gologger"
)

type Nats struct {
	configData  []byte
	conf        *koanf.Koanf
	multi       bool
	tags        []string
	connections map[string]*connection
}

type connection struct {
	Conn     *nats.Conn
	uri      string
	user     string
	password string
	Subs     map[string]*nats.Subscription
}

var NATS = &Nats{}
var logger = gologger.GetLogger()

func (m *Nats) Init(configData []byte) {
	m.configData = configData
	m.conf = koanf.New(".")
	if err := m.conf.Load(rawbytes.Provider(m.configData), yaml.Parser()); err != nil {
		logger.Error("load config failed, err: " + err.Error())
		return
	}
	if m.connections == nil || len(m.connections) == 0 {
		m.connections = make(map[string]*connection)
		m.tags = make([]string, 0)
		m.multi = m.conf.Bool("go.data.nats.multi")
		if m.multi {
			tags := strings.Split(m.conf.String("go.data.nats.conns"), ",")
			m.tags = tags
			for _, tag := range tags {
				conn := &connection{
					uri:      m.conf.String("go.data.nats." + tag + ".uri"),
					user:     m.conf.String("go.data.nats." + tag + ".user"),
					password: m.conf.String("go.data.nats." + tag + ".password"),
				}
				options := getDefaultNatsOptions()
				if conn.user != "" && conn.password != "" {
					options = append(options, nats.UserInfo(conn.user, conn.password))
				}

				c, err := nats.Connect(conn.uri, options...)
				if err != nil {
					logger.Error("connect Nats server failed, tag: " + tag + ", err: " + err.Error())
					continue
				}
				conn.Conn = c
				conn.Subs = make(map[string]*nats.Subscription)
				m.connections[tag] = conn
				logger.Info("connect Nats server success, tag: " + tag + ", server: " + conn.uri)
			}
		} else {
			conn := &connection{
				uri:      m.conf.String("go.data.nats.uri"),
				user:     m.conf.String("go.data.nats.user"),
				password: m.conf.String("go.data.nats.password"),
			}
			options := getDefaultNatsOptions()
			if conn.user != "" && conn.password != "" {
				options = append(options, nats.UserInfo(conn.user, conn.password))
			}

			c, err := nats.Connect(conn.uri, options...)
			if err != nil {
				logger.Error("connect Nats server failed, err: " + err.Error())
				return
			}
			conn.Conn = c
			conn.Subs = make(map[string]*nats.Subscription)
			m.connections["0"] = conn
			logger.Info("connect Nats server success, server: " + conn.uri)
		}
	}
}

// func (c *connection) onConnectHandler(client paho.Client) {
// 	if len(c.Topics) > 0 {
// 		logger.Info("重新连接成功，重新订阅主题...")
// 		for _, topic := range c.Topics {
// 			token := c.Client.Subscribe(topic.Topic, topic.Qos, safeHandler(*topic.HandlerFunc))
// 			if token.Error() != nil {
// 				logger.Error("subscribe topic failed, topic: " + topic.Topic + ", err: " + token.Error().Error())
// 				return
// 			}
// 			token.Wait()
// 		}
// 	}
// }

func (m *Nats) GetConnection(tag ...string) (*connection, error) {
	if !m.multi {
		if m.connections["0"].Conn.IsConnected() {
			return m.connections["0"], nil
		} else {
			conn := m.connections["0"]
			opts := getDefaultNatsOptions()
			if conn.user != "" && conn.password != "" {
				opts = append(opts, nats.UserInfo(conn.user, conn.password))
			}
			c, err := nats.Connect(conn.uri, opts...)
			if err != nil {
				logger.Error("reconnect Nats server failed, err: " + err.Error())
				return nil, err
			}
			conn.Conn = c
			m.connections["0"] = conn
			logger.Info("reconnect Nats server success, server: " + conn.uri)
			return conn, nil
		}
	}
	if len(tag) == 0 || tag[0] == "" {
		return nil, errors.New("tag is required for multi connections")
	}
	if _, ok := m.connections[tag[0]]; !ok {
		return nil, errors.New("connection not found for tag: " + tag[0])
	}
	if m.connections[tag[0]].Conn.IsConnected() {
		return m.connections[tag[0]], nil
	} else {
		conn := m.connections[tag[0]]
		opts := getDefaultNatsOptions()
		if conn.user != "" && conn.password != "" {
			opts = append(opts, nats.UserInfo(conn.user, conn.password))
		}
		c, err := nats.Connect(conn.uri, opts...)
		if err != nil {
			logger.Error("reconnect Nats server failed, err: " + err.Error())
			return nil, err
		}
		conn.Conn = c
		m.connections[tag[0]] = conn
		return conn, nil
	}

}

func (m *Nats) Close() {
	if !m.multi {
		if len(m.connections["0"].Subs) > 0 {
			for _, topic := range m.connections["0"].Subs {
				topic.Unsubscribe()
			}
		}
		m.connections["0"].Conn.Close()
		logger.Info("disconnect Nats server success, server: " + m.connections["0"].uri)
		delete(m.connections, "0")
	} else {
		for tag, _ := range m.connections {
			if len(m.connections[tag].Subs) > 0 {
				for _, topic := range m.connections[tag].Subs {
					topic.Unsubscribe()
				}
			}
			m.connections[tag].Conn.Close()
			logger.Info("disconnect Nats server success, tag: " + tag + ", server: " + m.connections[tag].uri)
			delete(m.connections, tag)
		}
	}
}

func (m *Nats) Check() error {
	var err error
	if m.multi {
		for tag, conn := range m.connections {
			if !conn.Conn.IsConnected() {
				logger.Error("Nats client not connected, tag: " + tag)
				opts := getDefaultNatsOptions()
				if conn.user != "" && conn.password != "" {
					opts = append(opts, nats.UserInfo(conn.user, conn.password))
				}
				c, err := nats.Connect(conn.uri, opts...)
				if err != nil {
					logger.Error("reconnect Nats server failed, err: " + err.Error())
					return err
				}
				conn.Conn = c
				m.connections[tag] = conn
			}
		}
	} else {
		conn := m.connections["0"]
		if !conn.Conn.IsConnected() {
			logger.Error("Nats client not connected")
			opts := getDefaultNatsOptions()
			if conn.user != "" && conn.password != "" {
				opts = append(opts, nats.UserInfo(conn.user, conn.password))
			}
			c, err := nats.Connect(conn.uri, opts...)
			if err != nil {
				logger.Error("reconnect Nats server failed, err: " + err.Error())
				return err
			}
			conn.Conn = c
			m.connections["0"] = conn
		}
	}
	return err
}

// safeHandler 包装MessageHandler，捕获panic
func safeHandler(handler nats.MsgHandler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Nats message handler panic: " + msg.Subject + ", error: " + r.(string))
			}
		}()
		handler(msg)
	}
}

func (m *Nats) Subscribe(tag, topic string, handlerFunc nats.MsgHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	subs, err := conn.Conn.Subscribe(topic, safeHandler(handlerFunc))
	if err != nil {
		logger.Error("subscribe topic failed, topic: " + topic + ", err: " + err.Error())
		return err
	}
	if tag == "" {
		tag = "0"
	}
	m.connections[tag].Subs[topic] = subs
	return nil
}

func (m *Nats) Publish(tag, topic string, payload []byte) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	if err := conn.Conn.Publish(topic, payload); err != nil {
		logger.Error("publish topic failed, topic: " + topic + ", err: " + err.Error())
		return err
	}
	return nil
}

func (m *Nats) SubscribeQueue(tag, topic, queueName string, handlerFunc nats.MsgHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	subs, err := conn.Conn.QueueSubscribe(topic, queueName, safeHandler(handlerFunc))
	if err != nil {
		logger.Error("subscribe topic failed, topic: " + topic + ", queueName: " + queueName + ", err: " + err.Error())
		return err
	}
	if tag == "" {
		tag = "0"
	}
	m.connections[tag].Subs[fmt.Sprintf("%s_%s", topic, queueName)] = subs
	return nil
}

func (m *Nats) UnSubscribe(tag string, topics ...string) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		conn.Subs[topic].Unsubscribe()
		delete(conn.Subs, topic)
	}
	m.connections[tag] = conn
	return nil
}

func getDefaultNatsOptions() []nats.Option {
	return []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(100),
		nats.ReconnectWait(time.Second),
	}
}

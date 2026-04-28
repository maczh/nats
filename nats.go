package nats

import (
	"errors"
	"math/rand"
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
	connns      []*nats.Conn
	connections map[string]*connection
}

type connection struct {
	conn     *nats.Conn
	uri      string
	user     string
	password string
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
				options := make([]nats.Option, 0)
				if conn.user != "" && conn.password != "" {
					options = append(options, nats.UserInfo(conn.user, conn.password))
				}

				c, err := nats.Connect(conn.uri, options...)
				if err != nil {
					logger.Error("connect Nats server failed, tag: " + tag + ", err: " + err.Error())
					continue
				}
				conn.conn = c
				m.connections[tag] = conn
				logger.Info("connect Nats server success, tag: " + tag + ", server: " + conn.server + ", clientId: " + conn.ClientId)
			}
		} else {
			conn := &connection{
				uri:      m.conf.String("go.data.nats.uri"),
				user:     m.conf.String("go.data.nats.user"),
				password: m.conf.String("go.data.nats.password"),
			}
			options := make([]nats.Option, 0)
			if conn.user != "" && conn.password != "" {
				options = append(options, nats.UserInfo(conn.user, conn.password))
			}

			c, err := nats.Connect(conn.uri, options...)
			if err != nil {
				logger.Error("connect Nats server failed, err: " + err.Error())
				return
			}
			conn.conn = c
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

func (m *Nats) GetConnection(tag ...string) (*nats.Conn, error) {
	if !m.multi {
		if m.connections["0"].conn.IsConnected() {
			return m.connections["0"].conn, nil
		} else {
			conn := m.connections["0"]
			opts := make([]nats.Option, 0)
			if conn.user != "" && conn.password != "" {
				opts = append(opts, nats.UserInfo(conn.user, conn.password))
			}
			c, err := nats.Connect(conn.uri, opts...)
			if err != nil {
				logger.Error("reconnect Nats server failed, err: " + err.Error())
				return nil, err
			}
			conn.conn = c
			m.connections["0"] = conn
			logger.Info("reconnect Nats server success, server: " + conn.uri)
			return conn.conn, nil
		}
	}
	if len(tag) == 0 || tag[0] == "" {
		return nil, errors.New("tag is required for multi connections")
	}
	if _, ok := m.connections[tag[0]]; !ok {
		return nil, errors.New("connection not found for tag: " + tag[0])
	}
	if m.connections[tag[0]].conn.IsConnected() {
		return m.connections[tag[0]].conn, nil
	} else {
		conn := m.connections[tag[0]]
		opts := make([]nats.Option, 0)
		if conn.user != "" && conn.password != "" {
			opts = append(opts, nats.UserInfo(conn.user, conn.password))
		}
		c, err := nats.Connect(conn.uri, opts...)
		if err != nil {
			logger.Error("reconnect Nats server failed, err: " + err.Error())
			return nil, err
		}
		conn.conn = c
		m.connections[tag[0]] = conn
		return conn.conn, nil
	}

}

func (m *Nats) Close() {
	if !m.multi {
		if len(m.connections["0"].Topics) > 0 {
			topics := make([]string, 0)
			for _, topic := range m.connections["0"].Topics {
				topics = append(topics, topic.Topic)
			}
			m.connections["0"].Client.Unsubscribe(topics...)
		}
		m.connections["0"].Client.Disconnect(0)
		logger.Info("disconnect Nats server success, server: " + m.connections["0"].server + ", clientId: " + m.connections["0"].ClientId)
		delete(m.connections, "0")
	} else {
		for tag, _ := range m.connections {
			if len(m.connections[tag].Topics) > 0 {
				topics := make([]string, 0)
				for _, topic := range m.connections[tag].Topics {
					topics = append(topics, topic.Topic)
				}
				m.connections[tag].Client.Unsubscribe(topics...)
			}
			m.connections[tag].Client.Disconnect(0)
			logger.Info("disconnect Nats server success, tag: " + tag + ", server: " + m.connections[tag].server + ", clientId: " + m.connections[tag].ClientId)
			delete(m.connections, tag)
		}
	}
}

func (m *Nats) Check() error {
	var err error
	if m.multi {
		for tag, conn := range m.connections {
			if !conn.conn.IsConnected() {
				logger.Error("Nats client not connected, tag: " + tag)
				opts := make([]nats.Option, 0)
				if conn.user != "" && conn.password != "" {
					opts = append(opts, nats.UserInfo(conn.user, conn.password))
				}
				c, err := nats.Connect(conn.uri, opts...)
				if err != nil {
					logger.Error("reconnect Nats server failed, err: " + err.Error())
					return err
				}
				conn.conn = c
				m.connections[tag] = conn
			}
		}
	} else {
		conn := m.connections["0"]
		if !conn.Client.IsConnected() {
			logger.Error("Nats client not connected")
			conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).Addserver(conn.server).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
			if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
				logger.Error("reconnect Nats server failed, err: " + token.Error().Error())
				err = token.Error()
			}
			m.connections["0"] = conn
		}
	}
	return err
}

// safeHandler 包装MessageHandler，捕获panic
// func safeHandler(handler nats.MsgHandler) nats.MsgHandler {
// 	return func(client *nats.Conn, msg *nats.Msg) {
// 		defer func() {
// 			if r := recover(); r != nil {
// 				logger.Error("Nats message handler panic: " + msg.Topic() + ", error: " + r.(string))
// 			}
// 		}()
// 		handler(msg)
// 	}
// }

func (m *Nats) Subscribe(tag, topic string, handlerFunc nats.MsgHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	subs, err := conn.Subscribe(topic, handlerFunc)
	if err != nil {
		logger.Error("subscribe topic failed, topic: " + topic + ", err: " + err.Error())
		return err
	}
	subs.Wait()
	if tag == "" {
		tag = "0"
	}
	m.connections[tag].Topics = append(m.connections[tag].Topics, SubTopics{Topic: topic, Qos: qos, HandlerFunc: &handlerFunc})
	return nil
}

func (m *Nats) SubscribeMultiple(tag string, filters map[string]byte, callback paho.MessageHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	token := conn.Client.SubscribeMultiple(filters, safeHandler(callback))
	if token.Error() != nil {
		logger.Error("subscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	token.Wait()
	if tag == "" {
		tag = "0"
	}
	for topic, _ := range filters {
		m.connections[tag].Topics = append(m.connections[tag].Topics, SubTopics{Topic: topic, Qos: filters[topic], HandlerFunc: &callback})
	}
	return nil
}

func (m *Nats) Publish(tag, topic string, qos byte, retained bool, payload interface{}) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	if token := conn.Client.Publish(topic, qos, retained, payload); token.Wait() && token.Error() != nil {
		logger.Error("publish topic failed, topic: " + topic + ", err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}
func (m *Nats) UnSubscribe(tag string, topics ...string) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	if token := conn.Client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		logger.Error("unsubscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}

func generateRandHexString(sl int) string {
	source := []byte("0123456789abcdef")
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < sl; i++ {
		result = append(result, source[r.Intn(len(source))])
	}
	return string(result)
}

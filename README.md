# MGin NATS客户端注册插件

## 安装

```bash
go get -u github.com/maczh/nats
```

## 使用

在MGin微服务模块的main.go中,在app := mgin.NewApp()之后，加入一行

```go
	//加载MQTT消息队列
    app.MGin.UsePlugin("nats", nats.NATS)
```

## yml配置
### 在MGin微服务模块本地配置文件中
```yaml
go:
  config:
    used: nats
    prefix:
      nats: nats
```

### 配置中心的nats-test.yml配置,单连接

配置文件中认证模式只能出现一种

```yaml
go:
  data:
    nats:
      multi: false
      uri: nats://127.0.0.1:4222
      username: username
      password: ********
      #token: ****token****
      #nkeys_seed: SUADZ3IDJGWTPBU36LV3PA2DGZSYYYUDJTEPAW2QDN4NECW4CFHTIMAKJQ
      #jwt_creds: test_user.creds
```
### 配置中心的nats-test.yml配置,多连接
```yaml
go:
  data:
    mqtt:
      multi: true
      conns: broker1, broker2
      broker1:
          uri: nats://127.0.0.1:4222
          username: username
          password: ********
          #token: ****token****
          #nkeys_seed: SUADZ3IDJGWTPBU36LV3PA2DGZSYYYUDJTEPAW2QDN4NECW4CFHTIMAKJQ
          #jwt_creds: test_user.creds
      broker2:
      uri: nats://192.168.xxx.xxx:4222
          username: username1
          password: ********
          #token: ****token****
          #nkeys_seed: SUADZ3IDJGWTPBU36LV3PA2DGZSYYYUDJTEPAW2QDN4NECW4CFHTIMAKJQ
          #jwt_creds: test_user1.creds
```


## 发送消息
```go
// 普通模式(无持久化)
nats.NATS.Publish("", "test.subject", []byte(msg))
// JetStream模式(持久化)
// 先创建Stream
nats.NATS.CreateStream("","stream1",[]string{"test.js.subj1","test.js.subj2"})
// 发送消息到stream
nats.NATS.PublishStream("","stream1",[]byte(msg),"test.js.subj2")
```

## 侦听主题消息并处理

- 普通模式订阅
```go
import (
    _nats "github.com/nats-io/nats.go"
    "github.com/maczh/mgin"
    "github.com/maczh/nats"
)
// 按队列模式订阅消费(同队列多消费者时，一条消息只能被消费一次)
err := nats.NATS.SubscribeQueue("", "test.>", "test.queue", func(msg *_nats.Msg) {
    fmt.Printf("接收到主题%s的消息:%s", msg.Subject, string(msg.Data))
})
// 按主题模式订阅(同主题多消费者，广播模式每条消息会被所有消费者接收到)
err := nats.NATS.Subscribe("", "test.>", func(msg *_nats.Msg) {
    fmt.Printf("接收到主题%s的消息:%s", msg.Subject, string(msg.Data))
})

```

- JetStream模式订阅
```go
	import (
    "github.com/nats-io/nats.go/jetstream"
    "github.com/maczh/mgin"
    "github.com/maczh/nats"
)
// 订阅jetStream消息
err := nats.NATS.SubscribeStream("", "stream1", "consumer1" , func(msg *jetstream.Msg) {
    meta,_ := msg.Metadata()
    fmt.Printf("接收到Stream[%s]主题%s的消息:%s", meta.Stream , msg.Subject(), string(msg.Data()))
})

```

## 各种认证对应的NATS服务端的nats-server.conf文件配置

### 1）用户密码认证

```
# Client port of 4222 on all interfaces
port: 4222

# HTTP monitoring port
monitor_port: 8222

authorization {
  users: [
    {user: test1, password: $2a$11$I6BvLFW419VCCH/VBzmjPOUn8SzB8CKDuI2kFos3/ZXIEw9gnlYYe },
    {user: test2, password: $2a$11$RVXHOKqYarUUdIA6DffLBefa1IefUpHyJ46H22zKhUHimALauypNu }
  ]
}

```

### 2）token认证

```
# Client port of 4222 on all interfaces
port: 4222

# HTTP monitoring port
monitor_port: 8222

authorization {
  token: amloYWkhMjAyNg==
}

```

### 3）NKey认证

```
# Client port of 4222 on all interfaces
port: 4222

# HTTP monitoring port
monitor_port: 8222

authorization {
  users: [
    {nkey: UC4FPL24GUP5RCSRWS4BA5CTSXI2GATT65JL34VGV5Z4ODRDNP6ZL56Z },
    {nkey: UDP26GOD43FXW33GNO3GZ6KSF45YHARTLQRXNHRQYR6BULGZVIPP6VZ3 }
  ]
}

```

### 4）JWT认证

```
# Client port of 4222 on all interfaces
port: 4222

# HTTP monitoring port
monitor_port: 8222

operator: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJDV0xIN0I3U0tHM0k0RjJYVjNRQUlCMjZJQTVVVFRWWU02UEJMQ09aNkNTM1NHUTJGVktRIiwiaWF0IjoxNzg2OTg2NzUwLCJpc3MiOiJPQ1pPS0tZUTRPQU5QM0tNQkpQRFRLTElCTUJHU01HTExXR0FCRVY3UUUyVUlXSVpTNUdJVVNNRSIsIm5hbWUiOiJteS1vcGVyYXRvciIsInN1YiI6Ik9DWk9LS1lRNE9BTlAzS01CSlBEVEtMSUJNQkdTTUdMTFdHQUJFVjdRRTJVSVdJWlM1R0lVU01FIiwibmF0cyI6eyJzaWduaW5nX2tleXMiOlsiT0RIVUxMSEJRQ1hGWEFDUkczWVFIUTJCREtTSVlDMktFMkFRRVdMNEdNTlNRUkZXNUlKQ0xIQ0YiXSwiYWNjb3VudF9zZXJ2ZXJfdXJsIjoibmF0czovL2xvY2FsaG9zdDo0MjIyIiwic3lzdGVtX2FjY291bnQiOiJBREpTRDVZRklURkFUN0s0VUs2RkVLRzdHVDNYNFZCUDRUR0xFV05JU1BOVVQ0TlJCUEhFRkZCUyIsInN0cmljdF9zaWduaW5nX2tleV91c2FnZSI6dHJ1ZSwidHlwZSI6Im9wZXJhdG9yIiwidmVyc2lvbiI6Mn19.aNeDWvXNEH1OosO-TUKsCHlb_U9xJjXRGvYB6nh2lft_1TD2mmGqCNWBDYydVbW-YnLUGsASwAU1NvIH7bagCA
# System Account named SYS
system_account: ADJSD5YFITFAT7K4UK6FEKG7GT3X4VBP4TGLEWNISPNUT4NRBPHEFFBS

# configuration of the nats based resolver
resolver {
    type: full
    # Directory in which the account jwt will be stored
    dir: './jwt'
    # In order to support jwt deletion, set to true
    # If the resolver type is full delete will rename the jwt.
    # This is to allow manual restoration in case of inadvertent deletion.
    # To restore a jwt, remove the added suffix .delete and restart or send a reload signal.
    # To free up storage you must manually delete files with the suffix .delete.
    allow_delete: false
    # Interval at which a nats-server with a nats based account resolver will compare
    # it's state with one random nats based account resolver in the cluster and if needed, 
    # exchange jwt and converge on the same set of jwt.
    interval: "2m"
    # Timeout for lookup requests in case an account does not exist locally.
    timeout: "1.9s"
}


# Preload the nats based resolver with the system account jwt.
# This is not necessary but avoids a bootstrapping system account. 
# This only applies to the system account. Therefore other account jwt are not included here.
# To populate the resolver:
# 1) make sure that your operator has the account server URL pointing at your nats servers.
#    The url must start with: "nats://" 
#    nsc edit operator --account-jwt-server-url nats://localhost:4222
# 2) push your accounts using: nsc push --all
#    The argument to push -u is optional if your account server url is set as described.
# 3) to prune accounts use: nsc push --prune 
#    In order to enable prune you must set above allow_delete to true
# Later changes to the system account take precedence over the system account jwt listed here.
resolver_preload: {
        ADJSD5YFITFAT7K4UK6FEKG7GT3X4VBP4TGLEWNISPNUT4NRBPHEFFBS: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiIzNklGTkRIVEhORFJGQkpHU0xHNE1UQTVSM1NIVzNWRE42MlQ2SFRHQ0dYQTNZUDNDQktRIiwiaWF0IjoxNzg2OTg2MzAxLCJpc3MiOiJPREhVTExIQlFDWEZYQUNSRzNZUUhRMkJES1NJWUMyS0UyQVFFV0w0R01OU1FSRlc1SUpDTEhDRiIsIm5hbWUiOiJTWVMiLCJzdWIiOiJBREpTRDVZRklURkFUN0s0VUs2RkVLRzdHVDNYNFZCUDRUR0xFV05JU1BOVVQ0TlJCUEhFRkZCUyIsIm5hdHMiOnsiZXhwb3J0cyI6W3sibmFtZSI6ImFjY291bnQtbW9uaXRvcmluZy1zdHJlYW1zIiwic3ViamVjdCI6IiRTWVMuQUNDT1VOVC4qLlx1MDAzZSIsInR5cGUiOiJzdHJlYW0iLCJhY2NvdW50X3Rva2VuX3Bvc2l0aW9uIjozLCJkZXNjcmlwdGlvbiI6IkFjY291bnQgc3BlY2lmaWMgbW9uaXRvcmluZyBzdHJlYW0iLCJpbmZvX3VybCI6Imh0dHBzOi8vZG9jcy5uYXRzLmlvL25hdHMtc2VydmVyL2NvbmZpZ3VyYXRpb24vc3lzX2FjY291bnRzIn0seyJuYW1lIjoiYWNjb3VudC1tb25pdG9yaW5nLXNlcnZpY2VzIiwic3ViamVjdCI6IiRTWVMuUkVRLkFDQ09VTlQuKi4qIiwidHlwZSI6InNlcnZpY2UiLCJyZXNwb25zZV90eXBlIjoiU3RyZWFtIiwiYWNjb3VudF90b2tlbl9wb3NpdGlvbiI6NCwiZGVzY3JpcHRpb24iOiJSZXF1ZXN0IGFjY291bnQgc3BlY2lmaWMgbW9uaXRvcmluZyBzZXJ2aWNlcyBmb3I6IFNVQlNaLCBDT05OWiwgTEVBRlosIEpTWiBhbmQgSU5GTyIsImluZm9fdXJsIjoiaHR0cHM6Ly9kb2NzLm5hdHMuaW8vbmF0cy1zZXJ2ZXIvY29uZmlndXJhdGlvbi9zeXNfYWNjb3VudHMifV0sImxpbWl0cyI6eyJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsIndpbGRjYXJkcyI6dHJ1ZSwiY29ubiI6LTEsImxlYWYiOi0xfSwic2lnbmluZ19rZXlzIjpbIkFCNTdGV0xIQlFFN05JQVY3WDdOR1lJSEtXWVRDWE9ONkZTT1REVTVLR1NEVkxDTzVKRFAyWEJGIl0sImRlZmF1bHRfcGVybWlzc2lvbnMiOnsicHViIjp7fSwic3ViIjp7fX0sImF1dGhvcml6YXRpb24iOnt9LCJ0eXBlIjoiYWNjb3VudCIsInZlcnNpb24iOjJ9fQ.fCbDwXJAEkI76iIMQ_6GElTud6qCB9F_cAo6PT1ZqxVLbcS8z9CcxH4CSuOfjet__suWJkAzdKWIS_HCofXlAA,
}

```


package core

// Message 消息接口（核心定义）
type Message interface {
	GetMsgId() int32
	GetData() interface{}
	GetConnection() Connection
	GetServerAck() int64 //客户端收到服务器ack序列号
	GetPacketType() int
}

// GlobalMessage 全局消息实现
type GlobalMessage struct {
	PacketType int
	MsgId      int32
	Data       interface{}
	ServerAck  int64
	Connection Connection
}

func (m *GlobalMessage) GetMsgId() int32 {
	return m.MsgId
}

func (m *GlobalMessage) GetData() interface{} {
	return m.Data
}

func (m *GlobalMessage) GetServerAck() int64 {
	return m.ServerAck
}

func (m *GlobalMessage) GetPacketType() int {
	return m.PacketType
}

func (m *GlobalMessage) GetConnection() Connection {
	return m.Connection
}

// HandlerFunc 消息处理器函数类型
type HandlerFunc func(conn Connection, entity interface{}, msg []byte) error

type EventHandlerFunc func(message Message) error

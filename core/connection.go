package core

import "google.golang.org/protobuf/proto"

const (
	NetworkProtocolType_TOKEN_CHECK = 1
	NetworkProtocolType_LOGIC       = 3
	NetworkProtocolType_HEART_BEAT  = 4
	NetworkProtocolType_SYNC_TIME   = 5
	NetworkProtocolType_FIXED_RATE  = 6
)

const (
	KickOutType_Unknown = iota
	KickOutType_Success
	KickOutType_SESSIONIDERROR
	KickOutType_ISPOSERROR
	KickOutType_DISCONNECTION
	KickOutType_HEART_OVERTIME
	KickOutType_PROTO_CACHE_MAX
	KickOutType_SYSTEMKICKOUT
	KickOutType_ACCOUNT_REPLACE_ERROR
	KickOutType_ACCOUNT_REPEAT_ERROR
	KickOutType_SYSTEM_MAINTAIN
	KickOutType_ACCOUNT_BAN_ERROR
	KickOutType_SERVER_MAINTAIN
)

// Connection 连接接口（核心定义）
type Connection interface {
	// 基本信息
	GetSessionId() int64
	SetPlayerGuid(playerGuid int64) //设置玩家的playerGuid
	GetPlayerGuid() int64           //获取玩家的playerGuid
	GetRemoteAddr() string
	GetLocalAddr() string
	IsClosed() bool

	//异步发送字节数组
	AsyncSend(data proto.Message) error
	//同步发送数据
	SyncSend(data []byte)

	Close() error
	Start() error

	//设置ServerAck
	SetServerAck(ack int64)

	// 设置客户端的ACK
	IncClientAck()
	// 获取客户端的ACK
	GetClientAck() int64

	// 数据包处理
	GetPacket() IPacket

	// 活动时间
	GetLastActivity() int64
	UpdateLastActivity()

	CheckServerAck(serverAck int64) bool

	UpdateServerAck(serverAck int64)

	// 获取客户端已经获取到服务器消息序号
	GetServerAck() int64

	//将未发送的消息全部给发送
	ProcessPendingMessages()
}

// ServeType 服务类型枚举
type ServeType int

const (
	Serve_Unknown ServeType = iota
	Serve_Server
	Serve_Client
)

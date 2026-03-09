package core

type ServerProto struct {
	clientAck       int64
	playerGuid      int64
	messageType     byte
	data            interface{}
	requestId       int32
	errorCode       int32
	protoType       byte
	messageDataType byte
}

func (sp *ServerProto) SetClientAck(clientAck int64) {
	sp.clientAck = clientAck
}

func (sp *ServerProto) SetPlayerGuid(playerGuid int64) {
	sp.playerGuid = playerGuid
}

func (sp *ServerProto) SetMessageType(messageType byte) {
	sp.messageType = messageType
}

func (sp *ServerProto) SetData(data interface{}) {
	sp.data = data
}

func (sp *ServerProto) SetRequestId(requestId int32) {
	sp.requestId = requestId
}

func (sp *ServerProto) SetErrorCode(errorCode int32) {
	sp.errorCode = errorCode
}

func (sp *ServerProto) SetProtoType(protoType byte) {
	sp.protoType = protoType
}

func (sp *ServerProto) SetMessageDataType(messageDataType byte) {
	sp.messageDataType = messageDataType
}

func (sp *ServerProto) GetClientAck() int64 {
	return sp.clientAck
}

func (sp *ServerProto) GetPlayerGuid() int64 {
	return sp.playerGuid
}

func (sp *ServerProto) GetMessageType() byte {
	return sp.messageType
}

func (sp *ServerProto) GetData() interface{} {
	return sp.data
}

func (sp *ServerProto) GetRequestId() int32 {
	return sp.requestId
}

func (sp *ServerProto) GetErrorCode() int32 {
	return sp.errorCode
}

func (sp *ServerProto) GetProtoType() byte {
	return sp.protoType
}

func (sp *ServerProto) GetMessageDataType() byte {
	return sp.messageDataType
}

const ProtoTypeProto = 1
const ProtoTypeBytes = 2

type ProtocolContentData struct {
	MsgId int32
	Pos   int64
	Data  []byte
}

func (pcd *ProtocolContentData) GetMsgId() int32 {
	return pcd.MsgId
}

func (pcd *ProtocolContentData) GetData() []byte {
	return pcd.Data
}
func (pcd *ProtocolContentData) GetIsPos() int64 {
	return pcd.Pos
}

type Token struct {
	Guid      int64  `json:"guid"`
	SessionId int64  `json:"sessionId"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Result    int    `json:"result"`
}

func (t *Token) GetGuid() int64 {
	return t.Guid
}

func (t *Token) GetSessionId() int64 {
	return t.SessionId
}

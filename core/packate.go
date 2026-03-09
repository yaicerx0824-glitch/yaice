package core

type IPacket interface {
	GetHeadLen() int
	GetSumLen() int
	Pack(data_ ServerProto) []byte
	HeartBeatPack(playerGuid int64) []byte
	SyncTimePack(playerGuid int64) []byte
	ReplyTokenPack(playerGuid int64, clientAck int64, statusCode int, processId int64) []byte
	Unpack(conn Connection, binaryData []byte) (error, func(conn Connection))
}

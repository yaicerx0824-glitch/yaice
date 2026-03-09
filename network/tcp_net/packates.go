package tcp_net

import (
	"bytes"
	"encoding/binary"
	"github.com/yaice-rx/yaice/core"
)

const (
	ConstMsgLength = 4 //消息长度
	ConstMsgIdLen  = 4
)

type packet struct {
}

func (dp *packet) HeartBeatPack(playerGuid int64) []byte {
	//TODO implement me
	panic("implement me")
}

func (dp *packet) SyncTimePack(playerGuid int64) []byte {
	//TODO implement me
	panic("implement me")
}

func (dp *packet) ReplyTokenPack(playerGuid int64, clientAck int64, statusCode int, processId int64) []byte {
	//TODO implement me
	panic("implement me")
}

func NewPacket() core.IPacket {
	return &packet{}
}

func (dp *packet) GetHeadLen() int {
	return ConstMsgLength
}

func (dp *packet) GetSumLen() int {
	return ConstMsgLength + ConstMsgIdLen
}

// 封包
func (dp *packet) Pack(data_ core.ServerProto) []byte {
	return nil
}

// 解包
func (dp *packet) Unpack(conn core.Connection, binaryData []byte) (error, func(conn core.Connection)) {
	//创建一个从输入二进制数据的ioReader
	dataBuff := bytes.NewReader(binaryData)
	//只解压head的信息，得到dataLen和msgID
	msg := &core.ProtocolContentData{}
	//读msgID
	if err := binary.Read(dataBuff, binary.BigEndian, &msg.MsgId); err != nil {
		return err, nil
	}
	//读msgID
	msg.Data = binaryData[ConstMsgIdLen:]
	//这里只需要把head的数据拆包出来就可以了，然后再通过head的长度，再从conn读取一次数据
	return nil, nil
}

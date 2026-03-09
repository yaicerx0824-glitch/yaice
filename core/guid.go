package core

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/yaicerx0824-glitch/yaice/config"
	"github.com/yaicerx0824-glitch/yaice/guid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"time"
)

type GuidManager struct {
	cfg *config.GrpcGuidConfig
}

// 单例模式
var guidManager *GuidManager

func GetGuidManager() *GuidManager {
	if guidManager == nil {
		guidManager = NewGuidManager(nil)
	}
	return guidManager
}

var highGuid int32

var lowGuid int32

func NewGuidManager(cfg *config.GrpcGuidConfig) *GuidManager {
	guidManager = &GuidManager{
		cfg: cfg,
	}
	return guidManager
}

func (g *GuidManager) GetGuid() int64 {
	if highGuid == 0 {
		lowGuid = 0
		g.highGuidRefresh()
	}

	lowGuid++
	if lowGuid == 2147483647 {
		lowGuid = 0
		g.highGuidRefresh()
	}

	tmp := int64(highGuid)
	tmp2 := int64(lowGuid)
	return tmp<<32 | tmp2
}

func (g *GuidManager) highGuidRefresh() int32 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		g.cfg.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	client := guid.NewHigh32AllocatorClient(conn)

	ts := fmt.Sprintf("%d", time.Now().Unix())
	payload := fmt.Sprintf("%s|%s|%s", g.cfg.Project, g.cfg.Env, ts)
	mac := hmac.New(sha256.New, []byte(g.cfg.ApiSecret))
	mac.Write([]byte(payload))
	sign := hex.EncodeToString(mac.Sum(nil))

	md := metadata.New(map[string]string{
		"x-timestamp": ts,
		"x-signature": sign,
	})
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	resp, err := client.Allocate(ctx, &guid.AllocateRequest{
		Project: g.cfg.Project,
		Env:     g.cfg.Env,
	})
	if err != nil {
		return 0
	}
	highGuid = int32(resp.Value)
	return highGuid
}

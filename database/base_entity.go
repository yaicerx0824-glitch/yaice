package database

import "go.mongodb.org/mongo-driver/v2/bson"

// BaseEntity 所有业务实体的基础结构
type BaseEntity struct {
	ID       bson.ObjectID `bson:"_id,omitempty" json:"id"`
	Guid     int64         `bson:"guid"`
	ShardKey int64         `bson:"shard_key"` // guid % 48
	Version  int64         `bson:"version"`   // 可选：乐观锁
}

func NewBaseEntity(guid int64) BaseEntity {
	return BaseEntity{
		Guid:     guid,
		ShardKey: guid % 48,
	}
}

type Entity interface {
	CollectionName() string
	Indexes() []IndexDefinition
}

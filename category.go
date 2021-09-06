package video

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type DataCategory struct {
	Id         string `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-" cql:"id"`
	Title      string `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty" cql:"title"`
	Assignable bool   `mapstructure:"assignable" json:"assignable,omitempty" gorm:"column:assignable" bson:"assignable,omitempty" dynamodbav:"assignable,omitempty" firestore:"assignable,omitempty" cql:"assignable"`
	ChannelId  string `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty" cql:"channelid"`
}

type Categories struct {
	Id   string         `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-"`
	Data []DataCategory `mapstructure:"data" json:"data,omitempty" gorm:"column:data" bson:"data,omitempty" dynamodbav:"data,omitempty" firestore:"data,omitempty"`
}

// phần này cho postgresql
func (c DataCategory) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *DataCategory) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &c)
}
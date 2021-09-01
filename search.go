package video

import (
	"time"
)

type ChannelSM struct {
	Q                 string     `mapstructure:"q" json:"q,omitempty" gorm:"column:q" bson:"q,omitempty" dynamodbav:"q,omitempty" firestore:"q"`
	Sort              string     `mapstructure:"sort" json:"sort,omitempty" gorm:"column:sort" bson:"sort,omitempty" dynamodbav:"sort,omitempty" firestore:"sort"`
	ChannelId         string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId"`
	ChannelType       string     `mapstructure:"channelType" json:"channelType,omitempty" gorm:"column:channelType" bson:"channelType,omitempty" dynamodbav:"channelType,omitempty" firestore:"channelType"`
	PublishedAfter    *time.Time `mapstructure:"publishedAfter" json:"publishedAfter,omitempty" gorm:"column:publishedAfter" bson:"publishedAfter,omitempty" dynamodbav:"publishedAfter,omitempty" firestore:"publishedAfter"`
	PublishedBefore   *time.Time `mapstructure:"publishedBefore" json:"publishedBefore,omitempty" gorm:"column:publishedBefore" bson:"publishedBefore,omitempty" dynamodbav:"publishedBefore,omitempty" firestore:"publishedBefore"`
	RegionCode        string     `mapstructure:"regionCode" json:"regionCode,omitempty" gorm:"column:regionCode" bson:"regionCode,omitempty" dynamodbav:"regionCode,omitempty" firestore:"regionCode"`
	RelevanceLanguage string     `mapstructure:"relevanceLanguage" json:"relevanceLanguage,omitempty" gorm:"column:relevanceLanguage" bson:"relevanceLanguage,omitempty" dynamodbav:"relevanceLanguage,omitempty" firestore:"relevanceLanguage"`
	SafeSearch        string     `mapstructure:"safeSearch" json:"safeSearch,omitempty" gorm:"column:safeSearch" bson:"safeSearch,omitempty" dynamodbav:"safeSearch,omitempty" firestore:"safeSearch"`
	TopicId           string     `mapstructure:"topicId" json:"topicId,omitempty" gorm:"column:topicId" bson:"topicId,omitempty" dynamodbav:"topicId,omitempty" firestore:"topicId"`
}

type PlaylistSM struct {
	Q                 string     `mapstructure:"q" json:"q,omitempty" gorm:"column:q" bson:"q,omitempty" dynamodbav:"q,omitempty" firestore:"q"`
	Sort              string     `mapstructure:"sort" json:"sort,omitempty" gorm:"column:sort" bson:"sort,omitempty" dynamodbav:"sort,omitempty" firestore:"sort"`
	ChannelId         string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId"`
	ChannelType       string     `mapstructure:"channelType" json:"channelType,omitempty" gorm:"column:channelType" bson:"channelType,omitempty" dynamodbav:"channelType,omitempty" firestore:"channelType"`
	PublishedAfter    *time.Time `mapstructure:"publishedAfter" json:"publishedAfter,omitempty" gorm:"column:publishedAfter" bson:"publishedAfter,omitempty" dynamodbav:"publishedAfter,omitempty" firestore:"publishedAfter"`
	PublishedBefore   *time.Time `mapstructure:"publishedBefore" json:"publishedBefore,omitempty" gorm:"column:publishedBefore" bson:"publishedBefore,omitempty" dynamodbav:"publishedBefore,omitempty" firestore:"publishedBefore"`
	RegionCode        string     `mapstructure:"regionCode" json:"regionCode,omitempty" gorm:"column:regionCode" bson:"regionCode,omitempty" dynamodbav:"regionCode,omitempty" firestore:"regionCode"`
	RelevanceLanguage string     `mapstructure:"relevanceLanguage" json:"relevanceLanguage,omitempty" gorm:"column:relevanceLanguage" bson:"relevanceLanguage,omitempty" dynamodbav:"relevanceLanguage,omitempty" firestore:"relevanceLanguage"`
	SafeSearch        string     `mapstructure:"safeSearch" json:"safeSearch,omitempty" gorm:"column:safeSearch" bson:"safeSearch,omitempty" dynamodbav:"safeSearch,omitempty" firestore:"safeSearch"`
}

type ItemSM struct {
	Q                 string     `mapstructure:"q" json:"q,omitempty" gorm:"column:q" bson:"q,omitempty" dynamodbav:"q,omitempty" firestore:"q"`
	Kind              string     `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind"`
	Duration          string     `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"durationomitempty" firestore:"duration"`
	Sort              string     `mapstructure:"sort" json:"sort,omitempty" gorm:"column:sort" bson:"sort,omitempty" dynamodbav:"sort,omitempty" firestore:"sort"`
	RelatedToVideoId  string     `mapstructure:"relatedToVideoId" json:"relatedToVideoId,omitempty" gorm:"column:relatedToVideoId" bson:"relatedToVideoId,omitempty" dynamodbav:"relatedToVideoId,omitempty" firestore:"relatedToVideoId"`
	ForMine           bool       `mapstructure:"forMine" json:"forMine,omitempty" gorm:"column:forMine" bson:"forMine,omitempty" dynamodbav:"forMine,omitempty" firestore:"forMine"`
	ChannelId         string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId"`
	ChannelType       string     `mapstructure:"channelType" json:"channelType,omitempty" gorm:"column:channelType" bson:"channelType,omitempty" dynamodbav:"channelType,omitempty" firestore:"channelType"`
	EventType         string     `mapstructure:"eventType" json:"eventType,omitempty" gorm:"column:eventType" bson:"eventType,omitempty" dynamodbav:"eventType,omitempty" firestore:"eventType"`
	PublishedAfter    *time.Time `mapstructure:"publishedAfter" json:"publishedAfter,omitempty" gorm:"column:publishedAfter" bson:"publishedAfter,omitempty" dynamodbav:"publishedAfter,omitempty" firestore:"publishedAfter"`
	PublishedBefore   *time.Time `mapstructure:"publishedBefore" json:"publishedBefore,omitempty" gorm:"column:publishedBefore" bson:"publishedBefore,omitempty" dynamodbav:"publishedBefore,omitempty" firestore:"publishedBefore"`
	RegionCode        string     `mapstructure:"regionCode" json:"regionCode,omitempty" gorm:"column:regionCode" bson:"regionCode,omitempty" dynamodbav:"regionCode,omitempty" firestore:"regionCode"`
	RelevanceLanguage string     `mapstructure:"relevanceLanguage" json:"relevanceLanguage,omitempty" gorm:"column:relevanceLanguage" bson:"relevanceLanguage,omitempty" dynamodbav:"relevanceLanguage,omitempty" firestore:"relevanceLanguage"`
	SafeSearch        string     `mapstructure:"safeSearch" json:"safeSearch,omitempty" gorm:"column:safeSearch" bson:"safeSearch,omitempty" dynamodbav:"safeSearch,omitempty" firestore:"safeSearch"`
	TopicId           string     `mapstructure:"topicId" json:"topicId,omitempty" gorm:"column:topicId" bson:"topicId,omitempty" dynamodbav:"topicId,omitempty" firestore:"topicId"`
	CategoryId        string     `mapstructure:"categoryId" json:"categoryId,omitempty" gorm:"column:categoryId" bson:"categoryId,omitempty" dynamodbav:"categoryId,omitempty" firestore:"categoryId,omitempty"`
	Caption           string     `mapstructure:"caption" json:"caption,omitempty" gorm:"column:caption" bson:"caption,omitempty" dynamodbav:"caption,omitempty" firestore:"caption"`
	Definition        string     `mapstructure:"definition" json:"definition,omitempty" gorm:"column:definition" bson:"definition,omitempty" dynamodbav:"definition,omitempty" firestore:"definition"`
	Dimension         string     `mapstructure:"dimension" json:"dimension,omitempty" gorm:"column:dimension" bson:"dimension,omitempty" dynamodbav:"dimension,omitempty" firestore:"dimension"`
	Embeddable        string     `mapstructure:"embeddable" json:"embeddable,omitempty" gorm:"embeddable:caption" bson:"embeddable,omitempty" dynamodbav:"embeddable,omitempty" firestore:"embeddable"`
	License           string     `mapstructure:"license" json:"license,omitempty" gorm:"column:license" bson:"license,omitempty" dynamodbav:"license,omitempty" firestore:"license"`
	Syndicated        string     `mapstructure:"syndicated" json:"syndicated,omitempty" gorm:"column:syndicated" bson:"syndicated,omitempty" dynamodbav:"syndicated,omitempty" firestore:"syndicated"`
	VideoType         string     `mapstructure:"videoType" json:"videoType,omitempty" gorm:"column:videoType" bson:"videoType,omitempty" dynamodbav:"videoType,omitempty" firestore:"videoType"`
}

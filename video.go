package video

import "time"

type ListResultVideos struct {
	List          []Video `mapstructure:"list" json:"list,omitempty" gorm:"column:list" bson:"list,omitempty" dynamodbav:"list,omitempty" firestore:"list,omitempty"`
	Total         int     `mapstructure:"total" json:"total,omitempty" gorm:"column:total" bson:"total,omitempty" dynamodbav:"total,omitempty" firestore:"total,omitempty"`
	Limit         int     `mapstructure:"limit" json:"limit,omitempty" gorm:"column:limit" bson:"limit,omitempty" dynamodbav:"limit,omitempty" firestore:"limit,omitempty"`
	NextPageToken string  `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type Video struct {
	Id                   string     `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-"`
	Caption              string     `mapstructure:"caption" json:"caption,omitempty" gorm:"column:caption" bson:"caption,omitempty" dynamodbav:"caption,omitempty" firestore:"caption,omitempty"`
	CategoryId           string     `mapstructure:"categoryId" json:"categoryId,omitempty" gorm:"column:categoryId" bson:"categoryId,omitempty" dynamodbav:"categoryId,omitempty" firestore:"categoryId,omitempty"`
	ChannelId            string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	ChannelTitle         string     `mapstructure:"channelTitle" json:"channelTitle,omitempty" gorm:"column:channelTitle" bson:"channelTitle,omitempty" dynamodbav:"channelTitle,omitempty" firestore:"channelTitle,omitempty"`
	Thumbnail            *string    `mapstructure:"thumbnail" json:"thumbnail,omitempty" gorm:"column:thumbnail" bson:"thumbnail,omitempty" dynamodbav:"thumbnail,omitempty" firestore:"thumbnail,omitempty"  cql:"thumbnail,omitempty"`
	MediumThumbnail      *string    `mapstructure:"mediumThumbnail" json:"mediumThumbnail,omitempty" gorm:"column:mediumThumbnail" bson:"mediumThumbnail,omitempty" dynamodbav:"mediumThumbnail,omitempty" firestore:"mediumThumbnail,omitempty" cql:"mediumthumbnail,omitempty"`
	HighThumbnail        *string    `mapstructure:"highThumbnail" json:"highThumbnail,omitempty" gorm:"column:highThumbnail" bson:"highThumbnail,omitempty" dynamodbav:"highThumbnail,omitempty" firestore:"highthumbnail,omitempty" cql:"highThumbnail,omitempty"`
	StandardThumbnail    *string    `mapstructure:"standardThumbnail" json:"standardThumbnail,omitempty" gorm:"column:standardThumbnail" bson:"standardThumbnail,omitempty" dynamodbav:"standardThumbnail,omitempty" firestore:"standardThumbnail,omitempty"`
	MaxresThumbnail      *string    `mapstructure:"maxresThumbnail" json:"maxresThumbnail,omitempty" gorm:"column:maxresThumbnail" bson:"maxresThumbnail,omitempty" dynamodbav:"maxresThumbnail,omitempty" firestore:"maxresThumbnail,omitempty"`
	DefaultAudioLanguage string     `mapstructure:"defaultAudioLanguage" json:"defaultAudioLanguage,omitempty" gorm:"column:defaultAudioLanguage" bson:"defaultAudioLanguage,omitempty" dynamodbav:"defaultAudioLanguage,omitempty" firestore:"defaultAudioLanguage,omitempty"`
	DefaultLanguage      string     `mapstructure:"defaultLanguage" json:"defaultLanguage,omitempty" gorm:"column:defaultLanguage" bson:"defaultLanguage,omitempty" dynamodbav:"defaultLanguage,omitempty" firestore:"defaultLanguage,omitempty"`
	Definition           int        `mapstructure:"definition" json:"definition,omitempty" gorm:"column:definition" bson:"definition,omitempty" dynamodbav:"definition,omitempty" firestore:"definition,omitempty"`
	Description          string     `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	Dimension            string     `mapstructure:"dimension" json:"dimension,omitempty" gorm:"column:dimension" bson:"dimension,omitempty" dynamodbav:"dimension,omitempty" firestore:"dimension,omitempty"`
	Duration             int64      `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	LicensedContent      *bool      `mapstructure:"licensedContent" json:"licensedContent,omitempty" gorm:"column:licensedContent" bson:"licensedContent,omitempty" dynamodbav:"licensedContent,omitempty" firestore:"licensedContent,omitempty"`
	LiveBroadcastContent string     `mapstructure:"liveBroadcastContent" json:"liveBroadcastContent,omitempty" gorm:"column:liveBroadcastContent" bson:"liveBroadcastContent,omitempty" dynamodbav:"liveBroadcastContent,omitempty" firestore:"liveBroadcastContent,omitempty"`
	LocalizedDescription string     `mapstructure:"localizedDescription" json:"localizedDescription,omitempty" gorm:"column:localizedDescription" bson:"localizedDescription,omitempty" dynamodbav:"localizedDescription,omitempty" firestore:"localizedDescription,omitempty"`
	LocalizedTitle       string     `mapstructure:"localizedTitle" json:"localizedTitle,omitempty" gorm:"column:localizedTitle" bson:"localizedTitle,omitempty" dynamodbav:"localizedTitle,omitempty" firestore:"localizedTitle,omitempty"`
	Projection           string     `mapstructure:"projection" json:"projection,omitempty" gorm:"column:projection" bson:"projection,omitempty" dynamodbav:"projection,omitempty" firestore:"projection,omitempty"`
	PublishedAt          *time.Time `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	Tags                 []string   `mapstructure:"tags" json:"tags,omitempty" gorm:"column:tags" bson:"tags,omitempty" dynamodbav:"tags,omitempty" firestore:"tags,omitempty"`
	Title                string     `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	BlockedRegions       []string   `mapstructure:"blockedRegions" json:"blockedRegions,omitempty" gorm:"column:blockedRegions" bson:"blockedRegions,omitempty" dynamodbav:"blockedRegions,omitempty" firestore:"blockedRegions,omitempty"`
	AllowedRegions       []string   `mapstructure:"allowedRegions" json:"allowedRegions,omitempty" gorm:"column:allowedRegions" bson:"allowedRegions,omitempty" dynamodbav:"allowedRegions,omitempty" firestore:"allowedRegions,omitempty"`
}

type VideoResult struct {
	Success   int        `mapstructure:"success" json:"success,omitempty" gorm:"column:success" bson:"success,omitempty" dynamodbav:"success,omitempty" firestore:"success,omitempty"`
	Count     int        `mapstructure:"count" json:"count,omitempty" gorm:"column:count" bson:"count,omitempty" dynamodbav:"count,omitempty" firestore:"count,omitempty"`
	All       int        `mapstructure:"all" json:"all,omitempty" gorm:"column:all" bson:"all,omitempty" dynamodbav:"all,omitempty" firestore:"position,omitempty"`
	Videos    []string   `mapstructure:"videos" json:"videos,omitempty" gorm:"column:videos" bson:"videos,omitempty" dynamodbav:"videos,omitempty" firestore:"videos,omitempty"`
	Timestamp *time.Time `mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
}

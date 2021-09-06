package youtube

import "time"

type PlaylistTubeResponse struct {
	Kind          string          `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag          string          `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	PageInfo      PageInfo        `mapstructure:"pageInfo" json:"pageInfo,omitempty" gorm:"column:pageInfo" bson:"pageInfo,omitempty" dynamodbav:"pageInfo,omitempty" firestore:"pageInfo,omitempty"`
	Items         []ItemsPlaylist `mapstructure:"items" json:"items,omitempty" gorm:"column:items" bson:"items,omitempty" dynamodbav:"items,omitempty" firestore:"items,omitempty"`
	NextPageToken string          `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type ItemsPlaylist struct {
	Kind           string                  `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag           string                  `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	Id             string                  `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Snippet        *SnippetPlaylist        `mapstructure:"snippet" json:"snippet,omitempty" gorm:"column:snippet" bson:"snippet,omitempty" dynamodbav:"snippet,omitempty" firestore:"snippet,omitempty"`
	ContentDetails *ContentDetailsPlaylist `mapstructure:"contentDetails" json:"contentDetails,omitempty" gorm:"column:contentDetails" bson:"contentDetails,omitempty" dynamodbav:"contentDetails,omitempty" firestore:"contentDetails,omitempty"`
}

type SnippetPlaylist struct {
	PublishedAt  time.Time          `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	ChannelId    string             `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	Title        string             `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Description  string             `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	Thumbnails   ThumbnailsPlaylist `mapstructure:"thumbnails" json:"thumbnails,omitempty" gorm:"column:thumbnails" bson:"thumbnails,omitempty" dynamodbav:"thumbnails,omitempty" firestore:"thumbnails,omitempty"`
	ChannelTitle string             `mapstructure:"channelTitle" json:"channelTitle,omitempty" gorm:"column:channelTitle" bson:"channelTitle,omitempty" dynamodbav:"channelTitle,omitempty" firestore:"channelTitle,omitempty"`
	Localized    Localized          `mapstructure:"localized" json:"localized,omitempty" gorm:"column:localized" bson:"localized,omitempty" dynamodbav:"localized,omitempty" firestore:"localized,omitempty"`
}

type ThumbnailsPlaylist struct {
	Default  ThumbnailItem `mapstructure:"default" json:"default,omitempty" gorm:"column:default" bson:"default,omitempty" dynamodbav:"default,omitempty" firestore:"default,omitempty"`
	Medium   ThumbnailItem `mapstructure:"medium" json:"medium,omitempty" gorm:"column:medium" bson:"medium,omitempty" dynamodbav:"medium,omitempty" firestore:"medium,omitempty"`
	High     ThumbnailItem `mapstructure:"high" json:"high,omitempty" gorm:"column:high" bson:"high,omitempty" dynamodbav:"high,omitempty" firestore:"high,omitempty"`
	Standard ThumbnailItem `mapstructure:"standard" json:"standard,omitempty" gorm:"column:standard" bson:"standard,omitempty" dynamodbav:"standard,omitempty" firestore:"standard,omitempty"`
	Maxres   ThumbnailItem `mapstructure:"maxres" json:"maxres,omitempty" gorm:"column:maxres" bson:"maxres,omitempty" dynamodbav:"maxres,omitempty" firestore:"maxres,omitempty"`
}

type ContentDetailsPlaylist struct {
	ItemCount int `mapstructure:"itemCount" json:"itemCount,omitempty" gorm:"column:itemCount" bson:"itemCount,omitempty" dynamodbav:"itemCount,omitempty" firestore:"itemCount,omitempty"`
}

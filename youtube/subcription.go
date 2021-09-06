package youtube

import "time"

type SubcriptionTubeResponse struct {
	Kind          string             `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind;primary_key" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag          string             `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag;primary_key" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	PageInfo      PageInfo           `mapstructure:"pageInfo" json:"pageInfo,omitempty" gorm:"column:pageInfo;primary_key" bson:"pageInfo,omitempty" dynamodbav:"pageInfo,omitempty" firestore:"pageInfo,omitempty"`
	Items         []ItemsSubcription `mapstructure:"items" json:"items,omitempty" gorm:"column:items;primary_key" bson:"items,omitempty" dynamodbav:"items,omitempty" firestore:"items,omitempty"`
	NextPageToken string             `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken;primary_key" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type ItemsSubcription struct {
	Kind           string                     `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind;primary_key" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag           string                     `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag;primary_key" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	Id             string                     `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Snippet        *SnippetPSubcription       `mapstructure:"snippet" json:"snippet,omitempty" gorm:"column:snippet;primary_key" bson:"snippet,omitempty" dynamodbav:"snippet,omitempty" firestore:"snippet,omitempty"`
	ContentDetails *ContentDetailsSubcription `mapstructure:"contentDetails" json:"contentDetails,omitempty" gorm:"column:contentDetails;primary_key" bson:"contentDetails,omitempty" dynamodbav:"contentDetails,omitempty" firestore:"contentDetails,omitempty"`
}

type SnippetPSubcription struct {
	PublishedAt time.Time             `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt;primary_key" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	Title       string                `mapstructure:"title" json:"title,omitempty" gorm:"column:title;primary_key" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Description string                `mapstructure:"description" json:"description,omitempty" gorm:"column:description;primary_key" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	ResourceId  ResourceIdSubcription `mapstructure:"resourceId" json:"resourceId,omitempty" gorm:"column:resourceId;primary_key" bson:"resourceId,omitempty" dynamodbav:"resourceId,omitempty" firestore:"resourceId,omitempty"`
	ChannelId   string                `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId;primary_key" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	Thumbnails  ThumbnailsPlaylist    `mapstructure:"thumbnails" json:"thumbnails,omitempty" gorm:"column:thumbnails;primary_key" bson:"thumbnails,omitempty" dynamodbav:"thumbnails,omitempty" firestore:"thumbnails,omitempty"`
}

type ResourceIdSubcription struct {
	Kind      string `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind;primary_key" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	ChannelId string `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId;primary_key" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
}
type ContentDetailsSubcription struct {
	TotalItemCount int    `mapstructure:"totalItemCount" json:"totalItemCount,omitempty" gorm:"column:totalItemCount;primary_key" bson:"totalItemCount,omitempty" dynamodbav:"totalItemCount,omitempty" firestore:"totalItemCount,omitempty"`
	NewItemCount   int    `mapstructure:"newItemCount" json:"newItemCount,omitempty" gorm:"column:newItemCount;primary_key" bson:"newItemCount,omitempty" dynamodbav:"newItemCount,omitempty" firestore:"newItemCount,omitempty"`
	ActivityType   string `mapstructure:"activityType" json:"activityType,omitempty" gorm:"column:activityType;primary_key" bson:"activityType,omitempty" dynamodbav:"activityType,omitempty" firestore:"activityType,omitempty"`
}

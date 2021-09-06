package youtube

import "time"

type VideoTubeResponse struct {
	Kind          string       `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag          string       `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	PageInfo      PageInfo     `mapstructure:"pageInfo" json:"pageInfo,omitempty" gorm:"column:pageInfo" bson:"pageInfo,omitempty" dynamodbav:"pageInfo,omitempty" firestore:"pageInfo,omitempty"`
	Items         []ItemsVideo `mapstructure:"items" json:"items,omitempty" gorm:"column:items" bson:"items,omitempty" dynamodbav:"items,omitempty" firestore:"items,omitempty"`
	NextPageToken string       `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type ItemsVideo struct {
	Kind           string               `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag           string               `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	Id             string               `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Snippet        *SnippetVideo        `mapstructure:"snippet" json:"snippet,omitempty" gorm:"column:snippet" bson:"snippet,omitempty" dynamodbav:"snippet,omitempty" firestore:"snippet,omitempty"`
	ContentDetails *ContentDetailsVideo `mapstructure:"contentDetails" json:"contentDetails,omitempty" gorm:"column:contentDetails" bson:"contentDetails,omitempty" dynamodbav:"contentDetails,omitempty" firestore:"contentDetails,omitempty"`
}

type SnippetVideo struct {
	PublishedAt          time.Time          `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	ChannelId            string             `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	Title                string             `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Description          string             `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	Thumbnails           ThumbnailsPlaylist `mapstructure:"thumbnails" json:"thumbnails,omitempty" gorm:"column:thumbnails" bson:"thumbnails,omitempty" dynamodbav:"thumbnails,omitempty" firestore:"thumbnails,omitempty"`
	ChannelTitle         string             `mapstructure:"channelTitle" json:"channelTitle,omitempty" gorm:"column:channelTitle" bson:"channelTitle,omitempty" dynamodbav:"channelTitle,omitempty" firestore:"channelTitle,omitempty"`
	Tags                 []string           `mapstructure:"tags" json:"tags,omitempty" gorm:"column:tags" bson:"tags,omitempty" dynamodbav:"tags,omitempty" firestore:"tags,omitempty"`
	CategoryId           string             `mapstructure:"categoryId" json:"categoryId,omitempty" gorm:"column:categoryId" bson:"categoryId,omitempty" dynamodbav:"categoryId,omitempty" firestore:"categoryId,omitempty"`
	LiveBroadcastContent string             `mapstructure:"liveBroadcastContent" json:"liveBroadcastContent,omitempty" gorm:"column:liveBroadcastContent" bson:"liveBroadcastContent,omitempty" dynamodbav:"liveBroadcastContent,omitempty" firestore:"liveBroadcastContent,omitempty"`
	Localized            Localized          `mapstructure:"localized" json:"localized,omitempty" gorm:"column:localized" bson:"localized,omitempty" dynamodbav:"localized,omitempty" firestore:"localized,omitempty"`
	DefaultLanguage      string             `mapstructure:"defaultLanguage" json:"defaultLanguage,omitempty" gorm:"column:defaultLanguage" bson:"defaultLanguage,omitempty" dynamodbav:"defaultLanguage,omitempty" firestore:"defaultLanguage,omitempty"`
	DefaultAudioLanguage string             `mapstructure:"defaultAudioLanguage" json:"defaultAudioLanguage,omitempty" gorm:"column:defaultAudioLanguage" bson:"defaultAudioLanguage,omitempty" dynamodbav:"defaultAudioLanguage,omitempty" firestore:"defaultAudioLanguage,omitempty"`
	PlaylistId           string             `mapstructure:"playlistId" json:"playlistId,omitempty" gorm:"column:playlistId" bson:"playlistId,omitempty" dynamodbav:"playlistId,omitempty" firestore:"playlistId,omitempty"`
	Position             int                `mapstructure:"position" json:"position,omitempty" gorm:"column:position" bson:"position,omitempty" dynamodbav:"position,omitempty" firestore:"position,omitempty"`
	// ResourceId             ResourceId         `mapstructure:"resourceId" json:"resourceId,omitempty" gorm:"column:resourceId" bson:"resourceId,omitempty" dynamodbav:"resourceId,omitempty" firestore:"resourceId,omitempty"`
	// VideoOwnerChannelTitle string             `mapstructure:"videoOwnerChannelTitle" json:"videoOwnerChannelTitle,omitempty" gorm:"column:videoOwnerChannelTitle" bson:"videoOwnerChannelTitle,omitempty" dynamodbav:"videoOwnerChannelTitle,omitempty" firestore:"videoOwnerChannelTitle,omitempty"`
	// VideoOwnerChannelId    string             `mapstructure:"videoOwnerChannelId" json:"videoOwnerChannelId,omitempty" gorm:"column:videoOwnerChannelId" bson:"videoOwnerChannelId,omitempty" dynamodbav:"videoOwnerChannelId,omitempty" firestore:"videoOwnerChannelId,omitempty"`
}

type ContentDetailsVideo struct {
	Duration          string            `mapstructure:"duration" json:"duration,omitempty" gorm:"column:duration" bson:"duration,omitempty" dynamodbav:"duration,omitempty" firestore:"duration,omitempty"`
	Dimension         string            `mapstructure:"dimension" json:"dimension,omitempty" gorm:"column:dimension" bson:"dimension,omitempty" dynamodbav:"dimension,omitempty" firestore:"dimension,omitempty"`
	Definition        string            `mapstructure:"definition" json:"definition,omitempty" gorm:"column:definition" bson:"definition,omitempty" dynamodbav:"definition,omitempty" firestore:"definition,omitempty"`
	Caption           string            `mapstructure:"caption" json:"caption,omitempty" gorm:"column:caption" bson:"caption,omitempty" dynamodbav:"caption,omitempty" firestore:"caption,omitempty"`
	LicensedContent   bool              `mapstructure:"licensedContent" json:"licensedContent,omitempty" gorm:"column:licensedContent" bson:"licensedContent,omitempty" dynamodbav:"licensedContent,omitempty" firestore:"licensedContent,omitempty"`
	ContentRating     interface{}       `mapstructure:"contentRating" json:"contentRating,omitempty" gorm:"column:contentRating" bson:"contentRating,omitempty" dynamodbav:"contentRating,omitempty" firestore:"contentRating,omitempty"`
	Projection        string            `mapstructure:"projection" json:"projection,omitempty" gorm:"column:projection" bson:"projection,omitempty" dynamodbav:"projection,omitempty" firestore:"projection,omitempty"`
	RegionRestriction RegionRestriction `mapstructure:"regionRestriction" json:"regionRestriction,omitempty" gorm:"column:regionRestriction" bson:"regionRestriction,omitempty" dynamodbav:"regionRestriction,omitempty" firestore:"regionRestriction,omitempty"`
}

type RegionRestriction struct {
	Allow   []string `mapstructure:"allow" json:"allow,omitempty" gorm:"column:allow" bson:"allow,omitempty" dynamodbav:"allow,omitempty" firestore:"allow,omitempty"`
	Blocked []string `mapstructure:"blocked" json:"blocked,omitempty" gorm:"column:blocked" bson:"blocked,omitempty" dynamodbav:"blocked,omitempty" firestore:"blocked,omitempty"`
}

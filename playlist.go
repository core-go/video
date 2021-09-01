package video

import "time"

type Playlist struct {
	Id                   string     `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-"`
	ChannelId            string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	ChannelTitle         string     `mapstructure:"channelTitle" json:"channelTitle,omitempty" gorm:"column:channelTitle" bson:"channelTitle,omitempty" dynamodbav:"channelTitle,omitempty" firestore:"channelTitle,omitempty"`
	Description          string     `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	Thumbnail            *string    `mapstructure:"thumbnail" json:"thumbnail,omitempty" gorm:"column:thumbnail" bson:"thumbnail,omitempty" dynamodbav:"thumbnail,omitempty" firestore:"thumbnail,omitempty"  cql:"thumbnail,omitempty"`
	MediumThumbnail      *string    `mapstructure:"mediumThumbnail" json:"mediumThumbnail,omitempty" gorm:"column:mediumThumbnail" bson:"mediumThumbnail,omitempty" dynamodbav:"mediumThumbnail,omitempty" firestore:"mediumThumbnail,omitempty" cql:"mediumthumbnail,omitempty"`
	HighThumbnail        *string    `mapstructure:"highThumbnail" json:"highThumbnail,omitempty" gorm:"column:highThumbnail" bson:"highThumbnail,omitempty" dynamodbav:"highThumbnail,omitempty" firestore:"highthumbnail,omitempty" cql:"highThumbnail,omitempty"`
	StandardThumbnail    *string    `mapstructure:"standardThumbnail" json:"standardThumbnail,omitempty" gorm:"column:standardThumbnail" bson:"standardThumbnail,omitempty" dynamodbav:"standardThumbnail,omitempty" firestore:"standardThumbnail,omitempty"`
	MaxresThumbnail      *string    `mapstructure:"maxresThumbnail" json:"maxresThumbnail,omitempty" gorm:"column:maxresThumbnail" bson:"maxresThumbnail,omitempty" dynamodbav:"maxresThumbnail,omitempty" firestore:"maxresThumbnail,omitempty"`
	LocalizedDescription string     `mapstructure:"localizedDescription" json:"localizedDescription,omitempty" gorm:"column:localizedDescription" bson:"localizedDescription,omitempty" dynamodbav:"localizedDescription,omitempty" firestore:"localizedDescription,omitempty"`
	LocalizedTitle       string     `mapstructure:"localizedTitle" json:"localizedTitle,omitempty" gorm:"column:localizedTitle" bson:"localizedTitle,omitempty" dynamodbav:"localizedTitle,omitempty" firestore:"localizedTitle,omitempty"`
	PublishedAt          *time.Time `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	Title                string     `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Count                *int       `mapstructure:"count" json:"count,omitempty" gorm:"column:count" bson:"count,omitempty" dynamodbav:"count,omitempty" firestore:"count,omitempty"`
	ItemCount            *int       `mapstructure:"itemCount" json:"itemCount,omitempty" gorm:"column:itemCount" bson:"itemCount,omitempty" dynamodbav:"itemCount,omitempty" firestore:"itemCount,omitempty"`
}

type ListResultPlaylist struct {
	List          []Playlist `mapstructure:"list" json:"list,omitempty" gorm:"column:list" bson:"list,omitempty" dynamodbav:"list,omitempty" firestore:"list,omitempty"`
	Total         int        `mapstructure:"total" json:"total,omitempty" gorm:"column:total" bson:"total,omitempty" dynamodbav:"total,omitempty" firestore:"total,omitempty"`
	Limit         int        `mapstructure:"limit" json:"limit,omitempty" gorm:"column:limit" bson:"limit,omitempty" dynamodbav:"limit,omitempty" firestore:"limit,omitempty"`
	NextPageToken string     `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type PlaylistResult struct {
	Count         int `mapstructure:"count" json:"count,omitempty" gorm:"column:count" bson:"count,omitempty" dynamodbav:"count,omitempty" firestore:"count,omitempty"`
	All           int `mapstructure:"all" json:"all,omitempty" gorm:"column:all" bson:"all,omitempty" dynamodbav:"all,omitempty" firestore:"all,omitempty"`
	VideoCount    int `mapstructure:"videoCount" json:"videoCount,omitempty" gorm:"column:videoCount" bson:"videoCount,omitempty" dynamodbav:"videoCount,omitempty" firestore:"videoCount,omitempty"`
	AllVideoCount int `mapstructure:"allVideoCount" json:"allVideoCount,omitempty" gorm:"column:allVideoCount" bson:"allVideoCount,omitempty" dynamodbav:"allVideoCount,omitempty" firestore:"allVideoCount,omitempty"`
}

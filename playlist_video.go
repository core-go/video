package video

import "time"

type PlaylistVideoIdVideos struct {
	Id     string   `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-"`
	Videos []string `mapstructure:"videos" json:"videos,omitempty" gorm:"column:videos" bson:"videos,omitempty" dynamodbav:"videos,omitempty" firestore:"videos,omitempty"`
}

type ListResultPlaylistVideo struct {
	List          []PlaylistVideo `mapstructure:"list" json:"list,omitempty" gorm:"column:list" bson:"list,omitempty" dynamodbav:"list,omitempty" firestore:"list,omitempty"`
	Total         int             `mapstructure:"total" json:"total,omitempty" gorm:"column:total" bson:"total,omitempty" dynamodbav:"total,omitempty" firestore:"total,omitempty"`
	Limit         int             `mapstructure:"limit" json:"limit,omitempty" gorm:"column:limit" bson:"limit,omitempty" dynamodbav:"limit,omitempty" firestore:"limit,omitempty"`
	NextPageToken string          `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type PlaylistVideo struct {
	Title                  string     `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"_title,omitempty" dynamodbav:"title,omitempty" firestore:"title"`
	Description            string     `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	LocalizedTitle         string     `mapstructure:"localizedTitle" json:"localizedTitle,omitempty" gorm:"column:localizedTitle" bson:"localizedTitle,omitempty" dynamodbav:"localizedTitle,omitempty" firestore:"localizedTitle,omitempty"`
	LocalizedDescription   string     `mapstructure:"localizedDescription" json:"localizedDescription,omitempty" gorm:"column:localizedDescription" bson:"localizedDescription,omitempty" dynamodbav:"localizedDescription,omitempty" firestore:"localizedDescription,omitempty"`
	ChannelId              string     `mapstructure:"channelId" json:"channelId,omitempty" gorm:"column:channelId" bson:"channelId,omitempty" dynamodbav:"channelId,omitempty" firestore:"channelId,omitempty"`
	ChannelTitle           string     `mapstructure:"channelTitle" json:"channelTitle,omitempty" gorm:"column:channelTitle" bson:"channelTitle,omitempty" dynamodbav:"channelTitle,omitempty" firestore:"channelTitle,omitempty"`
	Thumbnail              *string    `mapstructure:"thumbnail" json:"thumbnail,omitempty" gorm:"column:thumbnail" bson:"thumbnail,omitempty" dynamodbav:"thumbnail,omitempty" firestore:"thumbnail,omitempty"`
	MediumThumbnail        *string    `mapstructure:"mediumThumbnail" json:"mediumThumbnail,omitempty" gorm:"column:mediumThumbnail" bson:"mediumThumbnail,omitempty" dynamodbav:"mediumThumbnail,omitempty" firestore:"mediumThumbnail,omitempty"`
	HighThumbnail          *string    `mapstructure:"highThumbnail" json:"highThumbnail,omitempty" gorm:"column:highThumbnail" bson:"highThumbnail,omitempty" dynamodbav:"highThumbnail,omitempty" firestore:"highThumbnail,omitempty"`
	StandardThumbnail      *string    `mapstructure:"standardThumbnail" json:"standardThumbnail,omitempty" gorm:"column:standardThumbnail" bson:"standardThumbnail,omitempty" dynamodbav:"standardThumbnail,omitempty" firestore:"standardThumbnail,omitempty"`
	MaxresThumbnail        *string    `mapstructure:"maxresThumbnail" json:"maxresThumbnail,omitempty" gorm:"column:maxresThumbnail" bson:"maxresThumbnail,omitempty" dynamodbav:"maxresThumbnail,omitempty" firestore:"maxresThumbnail,omitempty"`
	Id                     string     `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-"`
	PublishedAt            *time.Time `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	PlaylistId             string     `mapstructure:"playlistId" json:"playlistId,omitempty" gorm:"column:playlistId" bson:"playlistId,omitempty" dynamodbav:"playlistId,omitempty" firestore:"playlistId,omitempty"`
	Position               int        `mapstructure:"position" json:"position,omitempty" gorm:"column:position" bson:"position,omitempty" dynamodbav:"position,omitempty" firestore:"position,omitempty"`
	VideoOwnerChannelId    string     `mapstructure:"videoOwnerChannelId" json:"videoOwnerChannelId,omitempty" gorm:"column:videoOwnerChannelId" bson:"videoOwnerChannelId,omitempty" dynamodbav:"videoOwnerChannelId,omitempty" firestore:"videoOwnerChannelId,omitempty"`
	VideoOwnerChannelTitle string     `mapstructure:"videoOwnerChannelTitle" json:"videoOwnerChannelTitle,omitempty" gorm:"column:videoOwnerChannelTitle" bson:"videoOwnerChannelTitle,omitempty" dynamodbav:"videoOwnerChannelTitle,omitempty" firestore:"videoOwnerChannelTitle,omitempty"`
}

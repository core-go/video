package video

import (
	_ "github.com/gocql/gocql"
	"time"
)

type ListResultChannel struct {
	List          []Channel `mapstructure:"list" json:"list,omitempty" gorm:"column:list" bson:"list,omitempty" dynamodbav:"list,omitempty" firestore:"list,omitempty"`
	Total         int       `mapstructure:"total" json:"total,omitempty" gorm:"column:total" bson:"total,omitempty" dynamodbav:"total,omitempty" firestore:"total,omitempty"`
	Limit         int       `mapstructure:"limit" json:"limit,omitempty" gorm:"column:limit" bson:"limit,omitempty" dynamodbav:"limit,omitempty" firestore:"limit,omitempty"`
	NextPageToken string    `mapstructure:"nextPageToken" json:"nextPageToken,omitempty" gorm:"column:nextPageToken" bson:"nextPageToken,omitempty" dynamodbav:"nextPageToken,omitempty" firestore:"nextPageToken,omitempty"`
}

type Channel struct {
	Id                     string     `mapstructure:"id" json:"id,omitempty" gorm:"column:id;primary_key" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"-" cql:"id,omitempty"`
	Count                  int        `mapstructure:"count" json:"count,omitempty" gorm:"column:count" bson:"count,omitempty" dynamodbav:"count,omitempty" firestore:"count,omitempty" cql:"count,omitempty"`
	Country                string     `mapstructure:"country" json:"country,omitempty" gorm:"column:country" bson:"country,omitempty" dynamodbav:"country,omitempty" firestore:"country,omitempty" cql:"country,omitempty"`
	CustomUrl              string     `mapstructure:"customUrl" json:"customUrl,omitempty" gorm:"column:customUrl" bson:"customUrl,omitempty" dynamodbav:"customUrl,omitempty" firestore:"customUrl,omitempty" cql:"customurl,omitempty"`
	Description            string     `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty" cql:"description,omitempty"`
	Favorites              string     `mapstructure:"favorites" json:"favorites,omitempty" gorm:"column:favorites" bson:"favorites,omitempty" dynamodbav:"favorites,omitempty" firestore:"favorites,omitempty" cql:"favorites,omitempty"`
	Thumbnail              *string    `mapstructure:"thumbnail" json:"thumbnail,omitempty" gorm:"column:thumbnail" bson:"thumbnail,omitempty" dynamodbav:"thumbnail,omitempty" firestore:"thumbnail,omitempty"  cql:"thumbnail,omitempty"`
	MediumThumbnail        *string    `mapstructure:"mediumThumbnail" json:"mediumThumbnail,omitempty" gorm:"column:mediumThumbnail" bson:"mediumThumbnail,omitempty" dynamodbav:"mediumThumbnail,omitempty" firestore:"mediumThumbnail,omitempty" cql:"mediumthumbnail,omitempty"`
	HighThumbnail          *string    `mapstructure:"highThumbnail" json:"highThumbnail,omitempty" gorm:"column:highThumbnail" bson:"highThumbnail,omitempty" dynamodbav:"highThumbnail,omitempty" firestore:"highthumbnail,omitempty" cql:"highThumbnail,omitempty"`
	ItemCount              int        `mapstructure:"itemCount" json:"itemCount,omitempty" gorm:"column:itemCount" bson:"itemCount,omitempty" dynamodbav:"itemCount,omitempty" firestore:"itemCount,omitempty" cql:"itemcount,omitempty"`
	Likes                  string     `mapstructure:"likes" json:"likes,omitempty" gorm:"column:likes" bson:"likes,omitempty" dynamodbav:"likes,omitempty" firestore:"likes,omitempty" cql:"likes,omitempty"`
	LocalizedDescription   string     `mapstructure:"localizedDescription" json:"localizedDescription,omitempty" gorm:"column:localizedDescription" bson:"localizedDescription,omitempty" dynamodbav:"localizedDescription,omitempty" firestore:"localizedDescription,omitempty" cql:"localizeddescription,omitempty"`
	LocalizedTitle         string     `mapstructure:"localizedTitle" json:"localizedTitle,omitempty" gorm:"column:localizedTitle" bson:"localizedTitle,omitempty" dynamodbav:"localizedTitle,omitempty" firestore:"localizedTitle,omitempty" cql:"localizedtitle,omitempty"`
	PlaylistCount          *int       `mapstructure:"playlistCount" json:"playlistCount,omitempty" gorm:"column:playlistCount" bson:"playlistCount,omitempty" dynamodbav:"playlistCount,omitempty" firestore:"playlistCount,omitempty" cql:"playlistcount,omitempty"`
	PlaylistItemCount      *int       `mapstructure:"playlistItemCount" json:"playlistItemCount,omitempty" gorm:"column:playlistItemCount" bson:"playlistItemCount,omitempty" dynamodbav:"playlistItemCount,omitempty" firestore:"playlistItemCount,omitempty" cql:"playlistitemcount,omitempty"`
	PlaylistVideoCount     *int       `mapstructure:"playlistVideoCount" json:"playlistVideoCount,omitempty" gorm:"column:playlistVideoCount" bson:"playlistVideoCount,omitempty" dynamodbav:"playlistVideoCount,omitempty" firestore:"playlistVideoCount,omitempty" cql:"playlistvideocount,omitempty"`
	PlaylistVideoItemCount *int       `mapstructure:"playlistVideoItemCount" json:"playlistVideoItemCount,omitempty" gorm:"column:playlistVideoItemCount" bson:"playlistVideoItemCount,omitempty" dynamodbav:"playlistVideoItemCount,omitempty" firestore:"playlistVideoItemCount,omitempty" cql:"playlistvideoitemcount,omitempty"`
	PublishedAt            *time.Time `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty" cql:"publishedat,omitempty"`
	LastUpload             *time.Time `mapstructure:"lastUpload" json:"lastUpload,omitempty" gorm:"column:lastUpload" bson:"lastUpload,omitempty" dynamodbav:"lastUpload,omitempty" firestore:"lastUpload,omitempty" cql:"lastupload,omitempty"`
	Title                  string     `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty" cql:"title,omitempty"`
	Uploads                string     `mapstructure:"uploads" json:"uploads,omitempty" gorm:"column:uploads" bson:"uploads,omitempty" dynamodbav:"uploads,omitempty" firestore:"uploads,omitempty" cql:"uploads,omitempty"`
	ChannelList            []string   `mapstructure:"channel_list" json:"-" gorm:"column:channels" bson:"channels,omitempty" dynamodbav:"channels,omitempty" firestore:"channels,omitempty" cql:"channels,omitempty"`
	Channels               []Channel  `mapstructure:"channels" json:"channels,omitempty" gorm:"-" bson:"-" dynamodbav:"-" firestore:"-" cql:"-"`
}

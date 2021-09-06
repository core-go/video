package youtube

import "time"

type ChannelTubeResponse struct {
	Kind     string         `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag     string         `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	PageInfo PageInfo       `mapstructure:"pageInfo" json:"pageInfo,omitempty" gorm:"column:pageInfo" bson:"pageInfo,omitempty" dynamodbav:"pageInfo,omitempty" firestore:"pageInfo,omitempty"`
	Items    []ItemsChannel `mapstructure:"items" json:"items,omitempty" gorm:"column:items" bson:"items,omitempty" dynamodbav:"items,omitempty" firestore:"items,omitempty"`
}

type PageInfo struct {
	TotalResults   int `mapstructure:"totalResults" json:"totalResults,omitempty" gorm:"column:totalResults" bson:"totalResults,omitempty" dynamodbav:"totalResults,omitempty" firestore:"totalResults,omitempty"`
	ResultsPerPage int `mapstructure:"resultsPerPage" json:"resultsPerPage,omitempty" gorm:"column:resultsPerPage" bson:"resultsPerPage,omitempty" dynamodbav:"resultsPerPage,omitempty" firestore:"resultsPerPage,omitempty"`
}

type ItemsChannel struct {
	Kind           string          `mapstructure:"kind" json:"kind,omitempty" gorm:"column:kind" bson:"kind,omitempty" dynamodbav:"kind,omitempty" firestore:"kind,omitempty"`
	Etag           string          `mapstructure:"etag" json:"etag,omitempty" gorm:"column:etag" bson:"etag,omitempty" dynamodbav:"etag,omitempty" firestore:"etag,omitempty"`
	Id             string          `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Snippet        *SnippetChannel `mapstructure:"snippet" json:"snippet,omitempty" gorm:"column:snippet" bson:"snippet,omitempty" dynamodbav:"snippet,omitempty" firestore:"snippet,omitempty"`
	ContentDetails *ContentDetails `mapstructure:"contentDetails" json:"contentDetails,omitempty" gorm:"column:contentDetails" bson:"contentDetails,omitempty" dynamodbav:"contentDetails,omitempty" firestore:"contentDetails,omitempty"`
}

type SnippetChannel struct {
	Title       string            `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Description string            `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
	CustomUrl   string            `mapstructure:"customUrl" json:"customUrl,omitempty" gorm:"column:customUrl" bson:"customUrl,omitempty" dynamodbav:"customUrl,omitempty" firestore:"customUrl,omitempty"`
	PublishedAt time.Time         `mapstructure:"publishedAt" json:"publishedAt,omitempty" gorm:"column:publishedAt" bson:"publishedAt,omitempty" dynamodbav:"publishedAt,omitempty" firestore:"publishedAt,omitempty"`
	Thumbnails  ThumbnailsChannel `mapstructure:"thumbnails" json:"thumbnails,omitempty" gorm:"column:thumbnails" bson:"thumbnails,omitempty" dynamodbav:"thumbnails,omitempty" firestore:"thumbnails,omitempty"`
	Localized   Localized         `mapstructure:"localized" json:"localized,omitempty" gorm:"column:localized" bson:"localized,omitempty" dynamodbav:"localized,omitempty" firestore:"localized,omitempty"`
	Country     string            `mapstructure:"country" json:"country,omitempty" gorm:"column:country" bson:"country,omitempty" dynamodbav:"country,omitempty" firestore:"country,omitempty"`
}

type ThumbnailsChannel struct {
	Default ThumbnailItem `mapstructure:"default" json:"default,omitempty" gorm:"column:default" bson:"default,omitempty" dynamodbav:"default,omitempty" firestore:"default,omitempty"`
	Medium  ThumbnailItem `mapstructure:"medium" json:"medium,omitempty" gorm:"column:medium" bson:"medium,omitempty" dynamodbav:"medium,omitempty" firestore:"medium,omitempty"`
	High    ThumbnailItem `mapstructure:"high" json:"high,omitempty" gorm:"column:high" bson:"high,omitempty" dynamodbav:"high,omitempty" firestore:"high,omitempty"`
}

type ThumbnailItem struct {
	Url    string `mapstructure:"url" json:"url,omitempty" gorm:"column:url" bson:"url,omitempty" dynamodbav:"url,omitempty" firestore:"url,omitempty"`
	Width  int    `mapstructure:"width" json:"width,omitempty" gorm:"column:width" bson:"width,omitempty" dynamodbav:"width,omitempty" firestore:"width,omitempty"`
	Height int    `mapstructure:"height" json:"height,omitempty" gorm:"column:height" bson:"height,omitempty" dynamodbav:"height,omitempty" firestore:"height,omitempty"`
}

type Localized struct {
	Title       string `mapstructure:"title" json:"title,omitempty" gorm:"column:title" bson:"title,omitempty" dynamodbav:"title,omitempty" firestore:"title,omitempty"`
	Description string `mapstructure:"description" json:"description,omitempty" gorm:"column:description" bson:"description,omitempty" dynamodbav:"description,omitempty" firestore:"description,omitempty"`
}

type ContentDetails struct {
	RelatedPlaylists RelatedPlaylists `mapstructure:"relatedPlaylists" json:"relatedPlaylists,omitempty" gorm:"column:relatedPlaylists" bson:"relatedPlaylists,omitempty" dynamodbav:"relatedPlaylists,omitempty" firestore:"relatedPlaylists,omitempty"`
}

type RelatedPlaylists struct {
	Likes     string `mapstructure:"likes" json:"likes,omitempty" gorm:"column:likes" bson:"likes,omitempty" dynamodbav:"likes,omitempty" firestore:"likes,omitempty"`
	Favorites string `mapstructure:"favorites" json:"favorites,omitempty" gorm:"column:favorites" bson:"favorites,omitempty" dynamodbav:"favorites,omitempty" firestore:"favorites,omitempty"`
	Uploads   string `mapstructure:"uploads" json:"uploads,omitempty" gorm:"column:uploads" bson:"uploads,omitempty" dynamodbav:"uploads,omitempty" firestore:"uploads,omitempty"`
}

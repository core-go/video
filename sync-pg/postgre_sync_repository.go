package pg

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/core-go/video"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"strings"
)

type PostgreVideoRepository struct {
	DB *sql.DB
}

func NewPostgreVideoRepository(db *sql.DB) *PostgreVideoRepository {
	return &PostgreVideoRepository{DB: db}
}

func (s *PostgreVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*video.ChannelSync, error) {
	rows, err := s.DB.Query(`select * from channelSync where id = $1 limit 1`, channelId)
	if err != nil {
		return nil, err
	}
	channelSync := video.ChannelSync{}
	for rows.Next() {
		err := rows.Scan(&channelSync.Id, &channelSync.Synctime, &channelSync.Uploads)
		if err != nil {
			return nil, err
		}
	}
	if channelSync.Id == ""{
		return nil, nil
	}
	return &channelSync, nil
}

func (s *PostgreVideoRepository) SaveChannel(ctx context.Context, channel video.Channel) (int64, error) {
	query := "insert into channel (id , count, country, customUrl, description , favorites, highThumbnail, itemCount, likes, localizedDescription, localizedTitle, mediumThumbnail, playlistCount , playlistItemCount, playlistVideoCount, playlistVideoItemCount, publishedAt, thumbnail, lastUpload, title ,uploads,channels) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12, $13, $14, $15,$16, $17, $18, $19, $20, $21,$22) ON CONFLICT (id) DO UPDATE SET count = $2, country = $3, customUrl = $4, description = $5, favorites = $6, highThumbnail = $7, itemCount = $8, likes = $9, localizedDescription = $10, localizedTitle = $11, mediumThumbnail = $12, playlistCount = $13, playlistItemCount = $14, playlistVideoCount = $15, playlistVideoItemCount = $16, publishedAt = $17, thumbnail = $18, lastUpload = $19, title = $20,uploads = $21,channels = $22"
	_, err := s.DB.Exec(query, channel.Id, channel.Count, channel.Country, channel.CustomUrl, channel.Description, channel.Favorites, channel.HighThumbnail, channel.ItemCount, channel.Likes, channel.LocalizedDescription, channel.LocalizedTitle, channel.MediumThumbnail, channel.PlaylistCount, channel.PlaylistItemCount, channel.PlaylistVideoCount, channel.PlaylistVideoItemCount, channel.PublishedAt, channel.Thumbnail, channel.LastUpload, channel.Title, channel.Uploads, pq.Array(channel.Channels))
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (s *PostgreVideoRepository) GetVideoIds(ctx context.Context, ids []string) ([]string, error) {
	var question []string
	var cc []interface{}
	for i, v := range ids {
		question = append(question, fmt.Sprintf("$%d", i+1))
		cc = append(cc, v)
	}
	query := fmt.Sprintf(`SELECT id FROM video WHERE id in (%s)`, strings.Join(question, ","))
	rows, err := s.DB.Query(query, cc...)
	if err != nil {
		return nil, err
	}
	var res []string
	for rows.Next() {
		var t string
		err := rows.Scan(&t)
		if err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func (s *PostgreVideoRepository) SaveVideos(ctx context.Context, videos []video.Video) (int, error) {
	log.Println("SaveVideos")
	tx, err := s.DB.Begin()
	if err != nil {
		return 0, err
	}
	for _, v := range videos {
		stmt := `INSERT INTO video (id,caption,categoryId,channelId,channelTitle,defaultAudioLanguage,defaultLanguage,definition,description,dimension,duration,highThumbnail,licensedContent,liveBroadcastContent,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,projection,publishedAt,standardThumbnail,tags,title,thumbnail,blockedRegions,allowedRegions) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26) ON CONFLICT (id) DO UPDATE SET caption = $2,categoryId = $3,channelId = $4,channelTitle = $5,defaultAudioLanguage = $6,defaultLanguage = $7,definition = $8,description = $9,dimension = $10,duration = $11,highThumbnail = $12,licensedContent = $13,liveBroadcastContent = $14,localizedDescription = $15,localizedTitle = $16,maxresThumbnail = $17,mediumThumbnail = $18,projection = $19,publishedAt = $20,standardThumbnail = $21,tags = $22,thumbnail = $23,title = $24,blockedRegions = $25,allowedRegions = $26`
		_, err = tx.Exec(stmt, v.Id, v.Caption, v.CategoryId, v.ChannelId, v.ChannelTitle, v.DefaultAudioLanguage, v.DefaultLanguage, v.Definition, v.Description, v.Dimension, v.Duration, v.HighThumbnail, v.LicensedContent, v.LiveBroadcastContent, v.LocalizedDescription, v.LocalizedTitle, v.MaxresThumbnail, v.MediumThumbnail, v.Projection, v.PublishedAt, v.StandardThumbnail, pq.Array(v.Tags), v.Title, v.Thumbnail, pq.Array(v.BlockedRegions), pq.Array(v.AllowedRegions))
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	return 1, err
}

func (s *PostgreVideoRepository) SavePlaylists(ctx context.Context, playlists []video.Playlist) (int, error) {
	fmt.Println("SavePlaylists")
	tx, err := s.DB.Begin()
	if err != nil {
		return 0, err
	}
	for _, v := range playlists {
		stmt := `INSERT INTO playlist (id,channelId,channelTitle,count,itemCount,description,highThumbnail,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,publishedAt,standardThumbnail,thumbnail,title) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (id) DO UPDATE SET channelId = $2,channelTitle = $3,count = $4,itemCount = $5,description = $6,highThumbnail = $7,localizedDescription = $8,localizedTitle = $9,maxresThumbnail = $10,mediumThumbnail = $11,publishedAt = $12,standardThumbnail = $13,thumbnail = $14,title = $15`
		_, err = tx.Exec(stmt, v.Id, v.ChannelId, v.ChannelTitle, v.Count, v.ItemCount, v.Description, v.HighThumbnail, v.LocalizedDescription, v.LocalizedTitle, v.MaxresThumbnail, v.MediumThumbnail, v.PublishedAt, v.StandardThumbnail, v.Thumbnail, v.Title)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	return 1, err
}

func (s *PostgreVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	fmt.Println("SavePlaylistVideos")
	playlistVideos := video.PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	query := "INSERT INTO playlistVideo(id, videos) values ($1, $2) ON CONFLICT (id) DO UPDATE SET videos = $2"
	_, err := s.DB.Exec(query, playlistVideos.Id, pq.Array(playlistVideos.Videos))
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (s *PostgreVideoRepository) SaveChannelSync(ctx context.Context, channel video.ChannelSync) (int, error) {
	fmt.Println("SaveChannelSync")
	query := "insert into channelSync (id,synctime,uploads) values ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET synctime = $2, uploads = $3"
	_, err := s.DB.Exec(query, channel.Id, channel.Synctime, channel.Uploads)
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

func (s *PostgreVideoRepository) SavePlaylist(ctx context.Context, playlist video.Playlist) (int, error) {
	fmt.Println("SavePlaylist")
	stmt := `INSERT INTO playlist (id,channelId,channelTitle,count,itemCount,description,highThumbnail,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,publishedAt,standardThumbnail,thumbnail,title) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (id) DO UPDATE SET channelId = $2,channelTitle = $3,count = $4,itemCount = $5,description = $6,highThumbnail = $7,localizedDescription = $8,localizedTitle = $9,maxresThumbnail = $10,mediumThumbnail = $11,publishedAt = $12,standardThumbnail = $13,thumbnail = $14,title = $15`
	_, err := s.DB.Exec(stmt, playlist.Id, playlist.ChannelId, playlist.ChannelTitle, playlist.Count, playlist.ItemCount, playlist.Description, playlist.HighThumbnail, playlist.LocalizedDescription, playlist.LocalizedTitle, playlist.MaxresThumbnail, playlist.MediumThumbnail, playlist.PublishedAt, playlist.StandardThumbnail, playlist.Thumbnail, playlist.Title)
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

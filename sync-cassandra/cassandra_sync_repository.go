package cassandra

import (
	"context"
	"fmt"
	. "github.com/core-go/video"
	"github.com/gocql/gocql"
	"strings"
)

type CassandraVideoRepository struct {
	Cassandra *gocql.ClusterConfig
}

func NewCassandraVideoRepository(cassandra *gocql.ClusterConfig) *CassandraVideoRepository {
	return &CassandraVideoRepository{
		Cassandra: cassandra,
	}
}

func (c *CassandraVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*ChannelSync, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	var channeSync ChannelSync
	query := `Select * from channelSync where id= ?`
	err := session.Query(query, channelId).Scan(&channeSync.Id, &channeSync.Synctime, &channeSync.Uploads)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		return nil, err
	}
	return &channeSync, err
}

func (c *CassandraVideoRepository) SaveChannel(ctx context.Context, channel Channel) (int64, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	query := "insert into channel (id , count, country, customUrl, description , favorites, highThumbnail, itemCount, likes, localizedDescription, localizedTitle, mediumThumbnail, playlistCount , playlistItemCount, playlistVideoCount, playlistVideoItemCount, publishedAt, thumbnail, lastUpload, title ,uploads, channels) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?)"
	err := session.Query(query, channel.Id, channel.Count, channel.Country, channel.CustomUrl, channel.Description, channel.Favorites, channel.HighThumbnail, channel.ItemCount, channel.Likes, channel.LocalizedDescription, channel.LocalizedTitle, channel.MediumThumbnail, channel.PlaylistCount, channel.PlaylistItemCount, channel.PlaylistVideoCount, channel.PlaylistVideoItemCount, channel.PublishedAt, channel.Thumbnail, channel.LastUpload, channel.Title, channel.Uploads, channel.Channels).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (c *CassandraVideoRepository) GetVideoIds(ctx context.Context, ids []string) ([]string, error) {
	session, er0 := c.Cassandra.CreateSession()
	var result []string
	if er0 != nil {
		return result, er0
	}
	var question []string
	var cc []interface{}
	for _, v := range ids {
		question = append(question, "?")
		cc = append(cc, v)
	}
	query := fmt.Sprintf(`SELECT id FROM video WHERE id in (%s)`, strings.Join(question, ","))
	rows := session.Query(query, cc...).Iter()
	var id string
	for rows.Scan(&id) {
		result = append(result, id)
	}
	return result, nil
}

func (c *CassandraVideoRepository) SaveVideos(ctx context.Context, videos []Video) (int, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	stmt := "INSERT INTO video (id,caption,categoryId,channelId,channelTitle,defaultAudioLanguage,defaultLanguage,definition,description,dimension,duration,highThumbnail,licensedContent,liveBroadcastContent,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,projection,publishedAt,standardThumbnail,tags,thumbnail,title,blockedRegions,allowedRegions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	for i := 0; i < len(videos); i++ {
		//caption, err := strconv.ParseBool(videos[i].Caption)
		//if err != nil {
		//	return 0, err
		//}
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       stmt,
			Args:       []interface{}{videos[i].Id, videos[i].Caption, videos[i].CategoryId, videos[i].ChannelId, videos[i].ChannelTitle, videos[i].DefaultAudioLanguage, videos[i].DefaultLanguage, videos[i].Definition, videos[i].Description, videos[i].Dimension, videos[i].Duration, videos[i].HighThumbnail, videos[i].LicensedContent, videos[i].LiveBroadcastContent, videos[i].LocalizedDescription, videos[i].LocalizedTitle, videos[i].MaxresThumbnail, videos[i].MediumThumbnail, videos[i].Projection, videos[i].PublishedAt, videos[i].StandardThumbnail, videos[i].Tags, videos[i].Thumbnail, videos[i].Title, videos[i].BlockedRegions, videos[i].AllowedRegions},
			Idempotent: true,
		})
		if i%5 == 0 || i == len(videos)-1 {
			err := session.ExecuteBatch(batch)
			if err != nil {
				return 0, err
			}
			batch = session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}
	}
	return 1, nil
}

func (c *CassandraVideoRepository) SavePlaylists(ctx context.Context, playlists []Playlist) (int, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	stmt := "INSERT INTO playlist (id,channelId,channelTitle,count,itemCount,description,highThumbnail,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,publishedAt,standardThumbnail,thumbnail,title) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	for i, playlist := range playlists {
		//batch.Query(stmt, playlist.Id, playlist.ChannelId, playlist.ChannelTitle, playlist.Count, playlist.ItemCount, playlist.Description, playlist.HighThumbnail, playlist.LocalizedDescription, playlist.LocalizedTitle, playlist.MaxresThumbnail, playlist.MediumThumbnail, playlist.PublishedAt, playlist.StandardThumbnail, playlist.Thumbnail, playlist.Title)
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       stmt,
			Args:       []interface{}{playlist.Id, playlist.ChannelId, playlist.ChannelTitle, playlist.Count, playlist.ItemCount, playlist.Description, playlist.HighThumbnail, playlist.LocalizedDescription, playlist.LocalizedTitle, playlist.MaxresThumbnail, playlist.MediumThumbnail, playlist.PublishedAt, playlist.StandardThumbnail, playlist.Thumbnail, playlist.Title},
			Idempotent: true,
		})
		if i%5 == 0 || i == len(playlists)-1 {
			err := session.ExecuteBatch(batch)
			if err != nil {
				return 0, err
			}
			batch = session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}
	}
	return 1, nil
}

func (c *CassandraVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	playlistVideos := PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	query := "INSERT INTO playlistVideo(id, videos) values (?, ?)"
	err := session.Query(query, playlistVideos.Id, playlistVideos.Videos).Exec()
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

func (c *CassandraVideoRepository) SaveChannelSync(ctx context.Context, channel ChannelSync) (int, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	query := "insert into channelSync (id,synctime,uploads) values (?, ?, ?)"
	err := session.Query(query, channel.Id, channel.Synctime, channel.Uploads).Exec()
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

func (c *CassandraVideoRepository) SavePlaylist(ctx context.Context, playlist Playlist) (int, error) {
	session, er0 := c.Cassandra.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	query := "insert into playlist (id,channelId,channelTitle,count,itemCount,description,highThumbnail,localizedDescription,localizedTitle,maxresThumbnail,mediumThumbnail,publishedAt,standardThumbnail,thumbnail,title) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	err := session.Query(query, playlist.Id, playlist.ChannelId, playlist.ChannelTitle, playlist.Count, playlist.ItemCount, playlist.Description, playlist.HighThumbnail, playlist.LocalizedDescription, playlist.LocalizedTitle, playlist.MaxresThumbnail, playlist.MediumThumbnail, playlist.PublishedAt, playlist.StandardThumbnail, playlist.Thumbnail, playlist.Title).Exec()
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

package cassandra

import (
	"context"
	"fmt"
	cassql "github.com/core-go/cassandra"
	. "github.com/core-go/video"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
)

type CassandraVideoRepository struct {
	session *gocql.Session
	channelSyncSchema *cassql.Schema
	indexFieldChannelSync map[string]int
	channelSchema *cassql.Schema
	playlistVideosSchema *cassql.Schema
	playlistSchema *cassql.Schema
	videoSchema *cassql.Schema
	indexFieldVideo map[string]int
}

func NewCassandraVideoRepository(cassandra *gocql.ClusterConfig) (*CassandraVideoRepository, error) {
	var channelSyncSc ChannelSync
	modelTypeChannelSync := reflect.TypeOf(channelSyncSc)
	indexFieldChannelSync,er0 := cassql.GetColumnIndexes(modelTypeChannelSync)
	if er0 != nil {
		return nil, er0
	}
	schemaChannelSync := cassql.CreateSchema(modelTypeChannelSync)

	var channelSc Channel
	modelTypeChannel := reflect.TypeOf(channelSc)
	schemaChannel := cassql.CreateSchema(modelTypeChannel)

	var playlistVideosSc PlaylistVideoIdVideos
	modelTypePlaylistVideos := reflect.TypeOf(playlistVideosSc)
	schemaPlaylistVideos := cassql.CreateSchema(modelTypePlaylistVideos)

	var playlistSc Playlist
	modelTypePlaylist := reflect.TypeOf(playlistSc)
	schemaPlaylist := cassql.CreateSchema(modelTypePlaylist)

	var videoSc Video
	modelTypeVideo := reflect.TypeOf(videoSc)
	schemaVideo := cassql.CreateSchema(modelTypeVideo)
	indexFieldVideo,er0 := cassql.GetColumnIndexes(modelTypeVideo)
	if er0 != nil {
		return nil, er0
	}

	session, er0 := cassandra.CreateSession()
	if er0 != nil {
		return nil, er0
	}

	return &CassandraVideoRepository{
		session: session,
		channelSyncSchema: schemaChannelSync,
		indexFieldChannelSync:indexFieldChannelSync,
		channelSchema: schemaChannel,
		playlistVideosSchema: schemaPlaylistVideos,
		playlistSchema: schemaPlaylist,
		videoSchema: schemaVideo,
		indexFieldVideo:indexFieldVideo,
	}, nil
}

func (c *CassandraVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*ChannelSync, error) {
	var channelSync []ChannelSync
	query := `Select * from channelSync where id= ?`
	err := cassql.Query(c.session, c.indexFieldChannelSync, &channelSync, query, channelId)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		return nil, err
	}
	lenList := len(channelSync)
	if lenList == 0 {
		return nil, nil
	}
	return &channelSync[0], err
}

func (c *CassandraVideoRepository) SaveChannel(ctx context.Context, channel Channel) (int64, error) {
	query, params := cassql.BuildToSave("channel", channel, c.channelSchema)
	res, err := cassql.Exec(c.session, query, params...)
	if err != nil {
		return -1, err
	}
	return res, nil
}

func (c *CassandraVideoRepository) GetVideoIds(ctx context.Context, ids []string) ([]string, error) {
	var video []Video
	var result []string
	var question []string
	var cc []interface{}
	for _, v := range ids {
		question = append(question, "?")
		cc = append(cc, v)
	}
	query := fmt.Sprintf(`SELECT * FROM video WHERE id in (%s)`, strings.Join(question, ","))
	err := cassql.Query(c.session, c.indexFieldVideo, &video, query, cc...)
	if err != nil {
		return nil, err
	}
	for i, _ := range video {
		result = append(result, video[i].Id)
	}
	return result, nil
}

func (c *CassandraVideoRepository) SaveVideos(ctx context.Context, videos []Video) (int, error) {
	stms, err := cassql.BuildToInsertOrUpdateBatch("video", videos, true, c.videoSchema)
	if err != nil {
		return -1, err
	}
	res, err := cassql.ExecuteAll(ctx, c.session, stms...)
	if err != nil {
		return -1, err
	}
	return int(res), nil
}

func (c *CassandraVideoRepository) SavePlaylists(ctx context.Context, playlists []Playlist) (int, error) {
	stms, err := cassql.BuildToInsertOrUpdateBatch("playlist", playlists, true, c.playlistSchema)
	if err != nil {
		return -1, err
	}
	res, err := cassql.ExecuteAll(ctx, c.session, stms...)
	if err != nil {
		return -1, err
	}
	return int(res), nil
}

func (c *CassandraVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	playlistVideos := PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	query, params := cassql.BuildToSave("playlistVideo", playlistVideos, c.playlistVideosSchema)
	res, err := cassql.Exec(c.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}

func (c *CassandraVideoRepository) SaveChannelSync(ctx context.Context, channel ChannelSync) (int, error) {
	query, params := cassql.BuildToSave("channelSync", channel, c.channelSyncSchema)
	res, err := cassql.Exec(c.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}

func (c *CassandraVideoRepository) SavePlaylist(ctx context.Context, playlist Playlist) (int, error) {
	query, params := cassql.BuildToSave("playlist", playlist, c.playlistSchema)
	res, err := cassql.Exec(c.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}
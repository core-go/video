package cassandra

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	c "github.com/core-go/cassandra"
	. "github.com/core-go/video"
	"github.com/gocql/gocql"
)

type CassandraVideoRepository struct {
	session               *gocql.Session
	channelSyncSchema     *c.Schema
	indexFieldChannelSync map[string]int
	channelSchema         *c.Schema
	playlistVideosSchema  *c.Schema
	playlistSchema        *c.Schema
	videoSchema           *c.Schema
	indexFieldVideo       map[string]int
}

func NewCassandraVideoRepository(cassandra *gocql.ClusterConfig) (*CassandraVideoRepository, error) {
	var channelSyncSc ChannelSync
	modelTypeChannelSync := reflect.TypeOf(channelSyncSc)
	indexFieldChannelSync, er0 := c.GetColumnIndexes(modelTypeChannelSync)
	if er0 != nil {
		return nil, er0
	}
	schemaChannelSync := c.CreateSchema(modelTypeChannelSync)

	var channelSc Channel
	modelTypeChannel := reflect.TypeOf(channelSc)
	schemaChannel := c.CreateSchema(modelTypeChannel)

	var playlistVideosSc PlaylistVideoIdVideos
	modelTypePlaylistVideos := reflect.TypeOf(playlistVideosSc)
	schemaPlaylistVideos := c.CreateSchema(modelTypePlaylistVideos)

	var playlistSc Playlist
	modelTypePlaylist := reflect.TypeOf(playlistSc)
	schemaPlaylist := c.CreateSchema(modelTypePlaylist)

	var videoSc Video
	modelTypeVideo := reflect.TypeOf(videoSc)
	schemaVideo := c.CreateSchema(modelTypeVideo)
	indexFieldVideo, er0 := c.GetColumnIndexes(modelTypeVideo)
	if er0 != nil {
		return nil, er0
	}

	session, er0 := cassandra.CreateSession()
	if er0 != nil {
		return nil, er0
	}

	return &CassandraVideoRepository{
		session:               session,
		channelSyncSchema:     schemaChannelSync,
		indexFieldChannelSync: indexFieldChannelSync,
		channelSchema:         schemaChannel,
		playlistVideosSchema:  schemaPlaylistVideos,
		playlistSchema:        schemaPlaylist,
		videoSchema:           schemaVideo,
		indexFieldVideo:       indexFieldVideo,
	}, nil
}

func (s *CassandraVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*ChannelSync, error) {
	var channelSync []ChannelSync
	query := `select * from channelSync where id= ?`
	err := c.Query(s.session, s.indexFieldChannelSync, &channelSync, query, channelId)
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

func (s *CassandraVideoRepository) SaveChannel(ctx context.Context, channel Channel) (int64, error) {
	query, params := c.BuildToSave("channel", channel, s.channelSchema)
	res, err := c.Exec(s.session, query, params...)
	if err != nil {
		return -1, err
	}
	return res, nil
}

func (s *CassandraVideoRepository) GetVideoIds(ctx context.Context, ids []string) ([]string, error) {
	var video []Video
	var result []string
	var question []string
	var cc []interface{}
	for _, v := range ids {
		question = append(question, "?")
		cc = append(cc, v)
	}
	query := fmt.Sprintf(`select * from video where id in (%s)`, strings.Join(question, ","))
	err := c.Query(s.session, s.indexFieldVideo, &video, query, cc...)
	if err != nil {
		return nil, err
	}
	for i, _ := range video {
		result = append(result, video[i].Id)
	}
	return result, nil
}

func (s *CassandraVideoRepository) SaveVideos(ctx context.Context, videos []Video) (int, error) {
	statements, err := c.BuildToInsertOrUpdateBatch("video", videos, true, s.videoSchema)
	if err != nil {
		return -1, err
	}
	res, err := c.ExecuteAll(ctx, s.session, statements...)
	if err != nil {
		return -1, err
	}
	return int(res), nil
}

func (s *CassandraVideoRepository) SavePlaylists(ctx context.Context, playlists []Playlist) (int, error) {
	statements, err := c.BuildToInsertOrUpdateBatch("playlist", playlists, true, s.playlistSchema)
	if err != nil {
		return -1, err
	}
	res, err := c.ExecuteAll(ctx, s.session, statements...)
	if err != nil {
		return -1, err
	}
	return int(res), nil
}

func (s *CassandraVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	playlistVideos := PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	query, params := c.BuildToSave("playlistVideo", playlistVideos, s.playlistVideosSchema)
	res, err := c.Exec(s.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}

func (s *CassandraVideoRepository) SaveChannelSync(ctx context.Context, channel ChannelSync) (int, error) {
	query, params := c.BuildToSave("channelSync", channel, s.channelSyncSchema)
	res, err := c.Exec(s.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}

func (s *CassandraVideoRepository) SavePlaylist(ctx context.Context, playlist Playlist) (int, error) {
	query, params := c.BuildToSave("playlist", playlist, s.playlistSchema)
	res, err := c.Exec(s.session, query, params...)
	if err != nil {
		return -1, nil
	}
	return int(res), nil
}

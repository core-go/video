package pg

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/core-go/video"
	"github.com/lib/pq"
)

type PostgreVideoRepository struct {
	DB                     *sql.DB
	fieldsIndexChannelSync map[string]int
	channelSchema          *Schema
	videoSchema            *Schema
	playlistSchema         *Schema
	channelSyncSchema      *Schema
	playlistVideoSchema    *Schema
}

func NewPostgreVideoRepository(db *sql.DB) (*PostgreVideoRepository, error) {
	var channelSync []video.ChannelSync
	modelType := reflect.TypeOf(channelSync).Elem()
	fieldsIndexChannelSync, er1 := GetColumnIndexes(modelType)
	if er1 != nil {
		return nil, er1
	}

	var channelSyncSc video.ChannelSync
	modelTypeChannelSync := reflect.TypeOf(channelSyncSc)
	schemaChannelSync := CreateSchema(modelTypeChannelSync)

	var channel video.Channel
	modelTypeChannel := reflect.TypeOf(channel)
	schemaChannel := CreateSchema(modelTypeChannel)

	var playlist video.Playlist
	modelTypePlaylist := reflect.TypeOf(playlist)
	schemaPlaylist := CreateSchema(modelTypePlaylist)

	var playlistVideo video.PlaylistVideoIdVideos
	modelTypePlaylistVideo := reflect.TypeOf(playlistVideo)
	schemaPlaylistVideo := CreateSchema(modelTypePlaylistVideo)

	var video video.Video
	modelTypeVideo := reflect.TypeOf(video)
	schemaVideo := CreateSchema(modelTypeVideo)

	return &PostgreVideoRepository{
		DB:                     db,
		fieldsIndexChannelSync: fieldsIndexChannelSync,
		channelSchema:          schemaChannel,
		videoSchema:            schemaVideo,
		playlistSchema:         schemaPlaylist,
		channelSyncSchema:      schemaChannelSync,
		playlistVideoSchema:    schemaPlaylistVideo,
	}, nil
}

func (s *PostgreVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*video.ChannelSync, error) {
	query := "select * from channelSync where id = $1 limit 1"
	var channelSyncRes []video.ChannelSync
	err := QueryWithMap(ctx, s.DB, s.fieldsIndexChannelSync, &channelSyncRes, query, channelId)
	if err != nil {
		return nil, err
	}

	if len(channelSyncRes) == 0 {
		return nil, nil
	}
	return &channelSyncRes[0], nil
}

func (s *PostgreVideoRepository) SaveChannel(ctx context.Context, channel video.Channel) (int64, error) {
	query, args, err1 := BuildToSaveWithArray("channel", channel, DriverPostgres, pq.Array, s.channelSchema)
	if err1 != nil {
		return 0, err1
	}
	_, err2 := s.DB.Exec(query, args...)
	if err2 != nil {
		return 0, err2
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
	query := fmt.Sprintf(`select id from video where id in (%s)`, strings.Join(question, ","))

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
	statements, err0 := BuildToSaveBatchWithArray("video", videos, DriverPostgres, pq.Array, s.videoSchema)
	if err0 != nil {
		return 0, err0
	}

	result, err := ExecuteAll(ctx, s.DB, statements...)
	if err != nil {
		return 0, err
	}

	return int(result), err
}

func (s *PostgreVideoRepository) SavePlaylists(ctx context.Context, playlists []video.Playlist) (int, error) {
	statements, err := BuildToSaveBatchWithArray("playlist", playlists, DriverPostgres, pq.Array, s.playlistSchema)
	if err != nil {
		return 0, err
	}

	result, err := ExecuteAll(ctx, s.DB, statements...)
	if err != nil {
		return 0, err
	}

	return int(result), err
}

func (s *PostgreVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	playlistVideos := video.PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	query, args, err1 := BuildToSaveWithArray("playlistVideo", playlistVideos, DriverPostgres, pq.Array, s.playlistVideoSchema)
	if err1 != nil {
		return 0, err1
	}
	_, err2 := s.DB.Exec(query, args...)
	if err2 != nil {
		return 0, err2
	}
	return 1, nil
}

func (s *PostgreVideoRepository) SaveChannelSync(ctx context.Context, channel video.ChannelSync) (int, error) {
	query, args, err1 := BuildToSaveWithArray("channelSync", channel, DriverPostgres, pq.Array, s.channelSyncSchema)
	if err1 != nil {
		return 0, err1
	}
	_, err2 := s.DB.Exec(query, args...)
	if err2 != nil {
		return 0, err2
	}
	return 1, nil
}

func (s *PostgreVideoRepository) SavePlaylist(ctx context.Context, playlist video.Playlist) (int, error) {
	statementss, err0 := BuildToSaveBatchWithArray("playlist", playlist, DriverPostgres, pq.Array, s.playlistSchema)
	if err0 != nil {
		return 0, err0
	}

	result, err := ExecuteAll(ctx, s.DB, statementss...)
	if err != nil {
		return 0, err
	}

	return int(result), err
}

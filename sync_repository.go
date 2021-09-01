package video

import (
	"context"
)

type SyncRepository interface {
	GetChannelSync(ctx context.Context, channelId string) (*ChannelSync, error)
	SaveChannel(ctx context.Context, channel Channel) (int64, error)
	SavePlaylist(ctx context.Context, playlist Playlist) (int, error)
	SavePlaylists(ctx context.Context, playlist []Playlist) (int, error)
	SaveChannelSync(ctx context.Context, channel ChannelSync) (int, error)
	SaveVideos(ctx context.Context, videos []Video) (int, error)
	SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error)
	GetVideoIds(ctx context.Context, id []string) ([]string, error)
}

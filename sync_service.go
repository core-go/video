package video

import "context"

type SyncService interface {
	SyncChannel(ctx context.Context, channelId string) (int, error)
	SyncChannels(ctx context.Context, channelIds []string) (int, error)
	SyncPlaylist(ctx context.Context, playlistId string, level *int) (int, error)
	SyncPlaylists(ctx context.Context, playlistIds []string,level int) (int,error)
	GetSubscriptions(ctx context.Context, channelId string) ([]Channel, error)
}

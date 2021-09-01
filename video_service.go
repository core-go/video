package video

import (
	"context"
)

type VideoService interface {
	GetChannel(ctx context.Context, channelId string, fields []string) (*Channel, error)
	GetChannels(ctx context.Context, ids []string, fields []string) (*[]Channel, error)
	GetPlaylist(ctx context.Context, id string, fields []string) (*Playlist, error)
	GetPlaylists(ctx context.Context, ids []string, fields []string) (*[]Playlist, error)
	GetVideo(ctx context.Context, id string, fields []string) (*Video, error)
	GetVideos(ctx context.Context, ids []string, fields []string) (*[]Video, error)
	GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*ListResultPlaylist, error)
	GetChannelVideos(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*ListResultVideos, error)
	GetPlaylistVideos(ctx context.Context, playlistId string, max int, nextPageToken string, fields []string) (*ListResultVideos, error)
	GetCagetories(ctx context.Context, regionCode string) (*Categories, error)
	SearchChannel(ctx context.Context, channelSM ChannelSM, max int, nextPageToken string, fields []string) (*ListResultChannel, error)
	SearchPlaylists(ctx context.Context, playlistSM PlaylistSM, max int, nextPageToken string, fields []string) (*ListResultPlaylist, error)
	SearchVideos(ctx context.Context, itemSM ItemSM, max int, nextPageToken string, fields []string) (*ListResultVideos, error)
	Search(ctx context.Context, itemSM ItemSM, max int, nextPageToken string, fields []string) (*ListResultVideos, error)
	GetRelatedVideos(ctx context.Context, videoId string, max int, nextPageToken string, fields []string) (*ListResultVideos, error)
	GetPopularVideos(ctx context.Context, regionCode string, categoryId string, limit int, nextPageToken string, fields []string) (*ListResultVideos, error)
}

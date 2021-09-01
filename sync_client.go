package video

type SyncClient interface {
	GetChannel(id string) (*Channel, error)
	GetChannels(ids []string) (*[]Channel, error)
	GetPlaylist(id string) (*Playlist, error)
	GetPlaylists(ids []string) (*[]Playlist, error)
	GetChannelPlaylists(channelId string, max int16, nextPageToken string) (*ListResultPlaylist, error)
	GetPlaylistVideos(playlistId string, max int16, nextPageToken string) (*ListResultPlaylistVideo, error)
	GetVideos(ids []string) (*ListResultVideos, error)
	GetSubscriptions(channelId string, mine string, max int, nextPageToken string) (*ListResultChannel, error)
}

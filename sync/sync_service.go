package sync

import (
	"context"
	"github.com/core-go/video"
	"github.com/core-go/video/youtube"
	"sync"
	"time"
)

type DefaultSyncService struct {
	Client     *youtube.YoutubeSyncClient
	Repository video.SyncRepository
}

func NewDefaultSyncService(client *youtube.YoutubeSyncClient, repository video.SyncRepository) *DefaultSyncService {
	return &DefaultSyncService{Client: client, Repository: repository}
}

func (d *DefaultSyncService) SyncChannel(ctx context.Context, channelId string) (int, error) {
	return syncChannel(ctx, d, channelId)
}

func (d *DefaultSyncService) SyncChannels(ctx context.Context, channelIds []string) (int, error) {
	var wg sync.WaitGroup
	var errSync error
	tam := 0
	for _, v := range channelIds {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			resC, err := d.SyncChannel(ctx, v)
			tam += resC
			if err != nil {
				errSync = err
			}
		}(&wg)
	}
	wg.Wait()
	if errSync != nil {
		return 0, errSync
	}
	return tam, errSync
}

func (d *DefaultSyncService) SyncPlaylist(ctx context.Context, playlistId string, level *int) (int, error) {
	var syncVideos bool
	if level != nil && *level < 2 {
		syncVideos = false
	} else {
		syncVideos = true
	}
	return syncPlaylist(ctx, playlistId, syncVideos, d)
}

func (d *DefaultSyncService) SyncPlaylists(ctx context.Context, playlistIds []string,level int) (int,error) {
	var wg sync.WaitGroup
	var errSync error
	tam := 0
	for _, v := range playlistIds {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			resC, err := d.SyncPlaylist(ctx, v, &level)
			tam += resC
			if err != nil {
				errSync = err
			}
		}(&wg)
	}
	wg.Wait()
	if errSync != nil {
		return 0, errSync
	}
	return tam, errSync
}

func (d *DefaultSyncService) GetSubscriptions(ctx context.Context, channelId string) ([]video.Channel, error) {
	channels := []video.Channel{}
	nextPageToken := ""
	flag := true
	mine := ""
	for flag {
		subscriptions, er0 := d.Client.GetSubscriptions(channelId, mine, 50, nextPageToken)
		if er0 != nil {
			return nil, er0
		}
		nextPageToken = subscriptions.NextPageToken
		if len(nextPageToken) <= 0 {
			flag = false
		}
		channels = append(channels, subscriptions.List...)
	}
	return channels, nil
}

func syncChannel(ctx context.Context, d *DefaultSyncService, channelId string) (int, error) {
	ChannelSync := make(chan *video.ChannelSync)
	errChannelSync := make(chan error)
	Channel := make(chan *video.Channel)
	errChannel := make(chan error)
	go func() {
		result, err := d.Repository.GetChannelSync(ctx, channelId)
		ChannelSync <- result
		errChannelSync <- err
	}()
	go func() {
		result, err := d.Client.GetChannel(channelId)
		Channel <- result
		errChannel <- err
	}()
	resultChannelSync := <-ChannelSync
	resultChannel := <-Channel
	er0 := <-errChannelSync
	er1 := <-errChannel
	if er0 != nil {
		return 0, er0
	}
	if er1 != nil {
		return 0, er1
	}
	result, er2 := checkAndSyncUpload(ctx, resultChannelSync, resultChannel, d)
	if er2 != nil {
		return 0, er2
	}
	return result, er2
}

func checkAndSyncUpload(ctx context.Context, channelSync *video.ChannelSync, channel *video.Channel, d *DefaultSyncService) (int, error) {
	if len(channel.Uploads) == 0 {
		return 0, nil
	} else {
		date := time.Now()
		var syncVideos bool
		var syncCollection bool
		var timestamp *time.Time
		if channelSync != nil {
			timestamp = channelSync.Synctime
		} else {
			timestamp = nil
		}
		if channelSync == nil || (channelSync != nil && channelSync.Level >= 2) {
			syncVideos = true
		} else {
			syncVideos = false
		}
		if channelSync == nil || (channelSync != nil && channelSync.Level >= 1) {
			syncCollection = true
		} else {
			syncCollection = false
		}
		rChan := make(chan *video.VideoResult)
		er1Chan := make(chan error)
		resultChan := make(chan *video.PlaylistResult)
		er2Chan := make(chan error)
		resSub := make(chan []video.Channel)
		er3Chan := make(chan error)
		go func() {
			res, err := syncUploads(ctx, channel.Uploads, d, timestamp)
			rChan <- res
			er1Chan <- err
		}()
		go func() {
			res, err := syncChannelPlaylists(ctx, channel.Id, syncVideos, syncCollection, d)
			resultChan <- res
			er2Chan <- err
		}()
		go func() {
			res, err := d.GetSubscriptions(ctx, channel.Id)
			resSub <- res
			er3Chan <- err
		}()
		r := <-rChan
		er1 := <-er1Chan
		result := <-resultChan
		er2 := <-er2Chan
		subChan := <-resSub
		er3 := <-er3Chan
		if er1 != nil {
			return 0, er1
		}
		if er2 != nil {
			return 0, er2
		}
		if er3 != nil {
			return 0, er3
		}
		channel.LastUpload = r.Timestamp
		channel.Count = r.Count
		channel.ItemCount = r.All
		for _, v := range subChan {
			channel.ChannelList = append(channel.ChannelList, v.Id)
		}
		if syncCollection {
			channel.PlaylistCount = &result.Count
			channel.PlaylistItemCount = &result.All
			channel.PlaylistVideoCount = &result.VideoCount
			channel.PlaylistVideoItemCount = &result.AllVideoCount
		}
		channelSync := video.ChannelSync{
			Id:       channel.Id,
			Synctime: &date,
			Uploads:  channel.Uploads,
		}
		er4Chan := make(chan error)
		go func() {
			_, err := d.Repository.SaveChannel(ctx, *channel)
			er4Chan <- err
		}()
		res, er5 := d.Repository.SaveChannelSync(ctx, channelSync)
		er4 := <-er4Chan
		if er4 != nil {
			return 0, er4
		}
		if er5 != nil {
			return 0, er5
		}
		return res, nil
	}
}

func syncChannelPlaylists(ctx context.Context, channelId string, syncVideos bool, saveCollection bool, d *DefaultSyncService) (*video.PlaylistResult, error) {
	nextPageToken := ""
	flag := true
	count := 0
	all := 0
	allVideoCount := 0
	for flag {
		channelPlaylists, er0 := d.Client.GetChannelPlaylists(channelId, 50, nextPageToken)
		if er0 != nil {
			return nil, er0
		}
		all = channelPlaylists.Total
		count = count + len(channelPlaylists.List)
		var playlistIds []string
		for _, v := range channelPlaylists.List {
			playlistIds = append(playlistIds, v.Id)
			allVideoCount = allVideoCount + *v.Count
		}
		nextPageToken = channelPlaylists.NextPageToken
		if nextPageToken == "" {
			flag = false
		}
		er1Chan := make(chan error)
		er2Chan := make(chan error)
		go func() {
			_, err := d.Repository.SavePlaylists(ctx, channelPlaylists.List)
			er1Chan <- err
		}()
		go func() {
			_, err := syncVideosOfPlaylists(ctx, playlistIds, syncVideos, saveCollection, d)
			er2Chan <- err
		}()
		//_,er2 := syncVideosOfPlaylists(ctx, playlistIds, syncVideos, saveCollection, d)
		er1 := <-er1Chan
		if er1 != nil {
			return nil, er1
		}
		er2 := <-er2Chan
		if er2 != nil {
			return nil, er2
		}
	}
	return &video.PlaylistResult{
		Count:         count,
		All:           all,
		AllVideoCount: allVideoCount,
	}, nil
}

func syncUploads(ctx context.Context, uploads string, d *DefaultSyncService, timestamp *time.Time) (*video.VideoResult, error) {
	nextPageToken := ""
	flag := true
	success := 0
	count := 0
	all := 0
	videoResult := video.VideoResult{}
	var last *time.Time
	for flag {
		playlistVideos, er1 := d.Client.GetPlaylistVideos(uploads, 50, nextPageToken)
		if er1 != nil {
			return nil, er1
		}
		all = playlistVideos.Total
		count = count + len(playlistVideos.List)
		if last == nil && len(playlistVideos.List) > 0 {
			last = playlistVideos.List[0].PublishedAt
		}
		newVideos := getNewVideos(playlistVideos.List, timestamp)
		if len(playlistVideos.List) > len(newVideos) {
			nextPageToken = ""
		} else {
			nextPageToken = playlistVideos.NextPageToken
		}
		if nextPageToken == "" {
			flag = false
		}
		r, er2 := saveVideos(ctx, newVideos, d)
		if er2 != nil {
			return nil, er2
		}
		success = success + r
	}
	videoResult.Count = success
	videoResult.All = all
	videoResult.Timestamp = last
	return &videoResult, nil
}

func getNewVideos(videos []video.PlaylistVideo, lastSynchronizedTime *time.Time) []video.PlaylistVideo {
	if lastSynchronizedTime == nil {
		return videos
	}
	timestamp := addSeconds(lastSynchronizedTime, -1800)
	t := int(timestamp.Unix())
	var newVideos []video.PlaylistVideo
	for _, i := range videos {
		if int(i.PublishedAt.Unix()) >= t {
			newVideos = append(newVideos, i)
		} else {
			return newVideos
		}
	}
	return newVideos
}

func addSeconds(date *time.Time, number int) *time.Time {
	newDate := time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second()-number, date.Nanosecond(), date.Location())
	return &newDate
}

func saveVideos(ctx context.Context, newVideos []video.PlaylistVideo, d *DefaultSyncService) (int, error) {
	if len(newVideos) == 0 || newVideos == nil {
		return 0, nil
	} else {
		if d == nil {
			return len(newVideos), nil
		} else {
			if d == nil {
				return len(newVideos), nil
			} else {
				var videoIds []string
				for _, v := range newVideos {
					videoIds = append(videoIds, v.Id)
				}
				ids, er0 := d.Repository.GetVideoIds(ctx, videoIds)
				if er0 != nil {
					return 0, er0
				}
				newIds := notIn(videoIds, ids)
				if len(newIds) == 0 {
					return 0, nil
				} else {
					videos, er1 := d.Client.GetVideos(newIds)
					if er1 != nil {
						return 0, er1
					}
					if videos != nil && len(videos.List) > 0 {
						res, er2 := d.Repository.SaveVideos(ctx, videos.List)
						if er2 != nil {
							return 0, er2
						}
						return res, nil
					} else {
						return 0, nil
					}
				}
			}
		}
	}
}

func syncVideosOfPlaylists(ctx context.Context, playlistIds []string, syncVideos bool, saveCollection bool, d *DefaultSyncService) (int, error) {
	sum := 0
	if saveCollection {
		for _, v := range playlistIds {
			resPlaylistVideos, er0 := syncPlaylistVideos(ctx, v, syncVideos, d)
			if er0 != nil {
				return 0, er0
			}
			res, er1 := d.Repository.SavePlaylistVideos(ctx, v, resPlaylistVideos.Videos)
			if er1 != nil {
				return 0, er1
			}
			sum = sum + res
		}
		return sum, nil
	} else {
		for _, v := range playlistIds {
			resPlaylistVideos, er0 := syncPlaylistVideos(ctx, v, syncVideos, d)
			if er0 != nil {
				return 0, er0
			}
			sum = sum + resPlaylistVideos.Success
		}
		return sum, nil
	}
}

func syncPlaylistVideos(ctx context.Context, playlistId string, syncVideos bool, d *DefaultSyncService) (*video.VideoResult, error) {
	nextPageToken := ""
	flag := true
	success := 0
	count := 0
	var newVideoIds []string
	for flag {
		playlistVideos, err := d.Client.GetPlaylistVideos(playlistId, 50, nextPageToken)
		if err != nil {
			return nil, err
		}
		count = count + len(playlistVideos.List)
		var videoIds []string
		for _, v := range playlistVideos.List {
			videoIds = append(videoIds, v.Id)
		}
		newVideoIds = append(newVideoIds, videoIds...)
		var def *DefaultSyncService
		if syncVideos {
			def = d
		} else {
			def = nil
		}
		r, er1 := saveVideos(ctx, playlistVideos.List, def)
		if er1 != nil {
			return nil, er1
		}
		success = success + r
		nextPageToken = playlistVideos.NextPageToken
		if nextPageToken == "" {
			flag = false
		}
	}
	return &video.VideoResult{
		Success: success,
		Count:   count,
		Videos:  newVideoIds,
	}, nil
}

func syncPlaylist(ctx context.Context, playlistId string, syncVideos bool, d *DefaultSyncService) (int, error) {
	resChan := make(chan *video.VideoResult)
	er0Chan := make(chan error)
	playlistChan := make(chan *video.Playlist)
	er1Chan := make(chan error)
	go func() {
		res, err := syncPlaylistVideos(ctx, playlistId, syncVideos, d)
		resChan <- res
		er0Chan <- err
	}()
	go func() {
		playlist, err := d.Client.GetPlaylist(playlistId)
		playlistChan <- playlist
		er1Chan <- err
	}()
	res := <-resChan
	er0 := <-er0Chan
	if er0 != nil {
		return 0, er0
	}
	playlist := <-playlistChan
	er1 := <-er1Chan
	if er1 != nil {
		return 0, er1
	}
	playlist.ItemCount = playlist.Count
	playlist.Count = &res.Count
	er2Chan := make(chan error)
	er3Chan := make(chan error)
	go func() {
		_, err := d.Repository.SavePlaylist(ctx, *playlist)
		er2Chan <- err
	}()
	go func() {
		_, err := d.Repository.SavePlaylistVideos(ctx, playlist.Id, res.Videos)
		er3Chan <- err
	}()
	//_,er3 := d.Repository.SavePlaylistVideos(ctx,playlist.Id,res.Videos)
	er2 := <-er2Chan
	er3 := <-er3Chan
	if er2 != nil {
		return 0, er2
	}
	if er3 != nil {
		return 0, er3
	}
	return res.Success, nil
}

func notIn(ids []string, subIds []string) []string {
	var newIds []string
	if len(subIds) == 0 {
		return ids
	}
	for _, v := range ids {
		flag := false
		for _, v1 := range subIds {
			if v == v1 {
				flag = true
				break
			}
		}
		if !flag {
			newIds = append(newIds, v)
		}
	}
	return newIds
}

package youtube

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	. "github.com/core-go/video"
)

type YoutubeSyncClient struct {
	Key string
}

func NewYoutubeSyncClient(key string) *YoutubeSyncClient {
	return &YoutubeSyncClient{Key: key}
}

func (y *YoutubeSyncClient) GetChannel(id string) (*Channel, error) {
	url := fmt.Sprintf(`https://www.googleapis.com/youtube/v3/channels?key=%s&id=%s&part=snippet,contentDetails`, y.Key, id)
	result, err := convertChannel(url)
	if err != nil {
		return nil, err
	}
	var channel Channel
	for _, v := range *result {
		channel = v
	}
	return &channel, err
}

func (y *YoutubeSyncClient) GetChannels(ids []string) (*[]Channel, error) {
	url := fmt.Sprintf(`https://www.googleapis.com/youtube/v3/channels?key=%s&id=%s&part=snippet,contentDetails`, y.Key, strings.Join(ids, ","))
	result, err := convertChannel(url)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (y *YoutubeSyncClient) GetPlaylist(id string) (*Playlist, error) {
	url := fmt.Sprintf(`https://youtube.googleapis.com/youtube/v3/playlists?key=%s&id=%s&part=snippet,contentDetails`, y.Key, id)
	result, err := convertPlaylist(url)
	if err != nil {
		return nil, err
	}
	return &result.List[0], err
}

func (y *YoutubeSyncClient) GetPlaylists(ids []string) (*[]Playlist, error) {
	url := fmt.Sprintf(`https://youtube.googleapis.com/youtube/v3/playlists?key=%s&id=%s&part=snippet,contentDetails`, y.Key, strings.Join(ids, ","))
	result, err := convertPlaylist(url)
	if err != nil {
		return nil, err
	}
	return &result.List, err
}

func (y *YoutubeSyncClient) GetChannelPlaylists(channelId string, max int16, nextPageToken string) (*ListResultPlaylist, error) {
	var maxResults int16
	var next string
	if max > 0 {
		maxResults = max
	} else {
		maxResults = 50
	}
	if nextPageToken != "" {
		next = fmt.Sprintf(`&pageToken=%s`, nextPageToken)
	} else {
		next = ""
	}
	url := fmt.Sprintf(`https://youtube.googleapis.com/youtube/v3/playlists?key=%s&channelId=%s&maxResults=%d%s&part=snippet,contentDetails`, y.Key, channelId, maxResults, next)
	result, err := convertPlaylist(url)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (y *YoutubeSyncClient) GetPlaylistVideos(playlistId string, max int16, nextPageToken string) (*ListResultPlaylistVideo, error) {
	var maxResults int16
	var next string
	if max > 0 {
		maxResults = max
	} else {
		maxResults = 50
	}
	if nextPageToken != "" {
		next = fmt.Sprintf(`&pageToken=%s`, nextPageToken)
	} else {
		next = ""
	}
	url := fmt.Sprintf(`https://youtube.googleapis.com/youtube/v3/playlistItems?key=%s&playlistId=%s&maxResults=%d%s&part=snippet,contentDetails`, y.Key, playlistId, maxResults, next)
	result, err := convertPlaylistVideo(url)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (y *YoutubeSyncClient) GetVideos(ids []string) (*ListResultVideos, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	url := fmt.Sprintf(`https://www.googleapis.com/youtube/v3/videos?key=%s&part=snippet,contentDetails&id=%s`, y.Key, strings.Join(ids, ","))
	result, err := convertVideos(url)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (y *YoutubeSyncClient) GetSubscriptions(channelId string, mine string, max int, nextPageToken string) (*ListResultChannel, error) {
	var maxResult int
	var pageToken string
	var mineStr string
	var channel string
	if max > 0 {
		maxResult = max
	} else {
		maxResult = 50
	}
	if len(nextPageToken) > 0 {
		pageToken = fmt.Sprintf(`&pageToken=%s`, nextPageToken)
	} else {
		pageToken = ""
	}
	if len(mine) > 0 {
		mineStr = fmt.Sprintf(`&mine=%s`, mine)
	} else {
		mineStr = ""
	}
	if len(channelId) > 0 {
		channel = fmt.Sprintf(`&channelId=%s`, channelId)
	} else {
		channel = ""
	}
	url := fmt.Sprintf(`https://youtube.googleapis.com/youtube/v3/subscriptions?key=%s%s%s&maxResults=%d%s&part=snippet`, y.Key, mineStr, channel, maxResult, pageToken)
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary SubcriptionTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	var channels ListResultChannel
	channels.NextPageToken = summary.NextPageToken
	for _, v := range summary.Items {
		var chann Channel
		chann.Id = v.Snippet.ResourceId.ChannelId
		chann.Title = v.Snippet.Title
		chann.Description = v.Snippet.Description
		chann.PublishedAt = &v.Snippet.PublishedAt
		chann.Thumbnail = &v.Snippet.Thumbnails.Default.Url
		chann.MediumThumbnail = &v.Snippet.Thumbnails.Medium.Url
		chann.HighThumbnail = &v.Snippet.Thumbnails.High.Url
		channels.List = append(channels.List, chann)
	}
	return &channels, nil
}

func convertChannel(url string) (*[]Channel, error) {
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary ChannelTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	channel := make([]Channel, len(summary.Items))
	for i, v := range summary.Items {
		log.Println(v)
		channel[i].Id = v.Id
		channel[i].Title = v.Snippet.Title
		channel[i].Description = v.Snippet.Description
		channel[i].PublishedAt = &v.Snippet.PublishedAt
		channel[i].CustomUrl = v.Snippet.CustomUrl
		channel[i].Country = v.Snippet.Country
		channel[i].LocalizedTitle = v.Snippet.Localized.Title
		channel[i].LocalizedDescription = v.Snippet.Localized.Description
		channel[i].Thumbnail = &v.Snippet.Thumbnails.Default.Url
		channel[i].MediumThumbnail = &v.Snippet.Thumbnails.Medium.Url
		channel[i].HighThumbnail = &v.Snippet.Thumbnails.High.Url
		channel[i].Uploads = v.ContentDetails.RelatedPlaylists.Uploads
		channel[i].Likes = v.ContentDetails.RelatedPlaylists.Likes
		channel[i].Favorites = v.ContentDetails.RelatedPlaylists.Favorites
	}
	return &channel, nil
}

func convertPlaylist(url string) (*ListResultPlaylist, error) {
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary PlaylistTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	listResultPlaylist := ListResultPlaylist{}
	listResultPlaylist.Total = summary.PageInfo.TotalResults
	listResultPlaylist.Limit = summary.PageInfo.ResultsPerPage
	if summary.NextPageToken != "" {
		listResultPlaylist.NextPageToken = summary.NextPageToken
	}
	for _, v := range summary.Items {
		playlist := Playlist{}
		playlist.Id = v.Id
		playlist.Title = v.Snippet.Title
		playlist.Description = v.Snippet.Description
		playlist.PublishedAt = &v.Snippet.PublishedAt
		playlist.LocalizedTitle = v.Snippet.Localized.Title
		playlist.LocalizedDescription = v.Snippet.Localized.Description
		playlist.ChannelId = v.Snippet.ChannelId
		playlist.ChannelTitle = v.Snippet.ChannelTitle
		playlist.Count = &v.ContentDetails.ItemCount
		playlist.Thumbnail = &v.Snippet.Thumbnails.Default.Url
		playlist.MediumThumbnail = &v.Snippet.Thumbnails.Medium.Url
		playlist.HighThumbnail = &v.Snippet.Thumbnails.High.Url
		playlist.StandardThumbnail = &v.Snippet.Thumbnails.Standard.Url
		playlist.MaxresThumbnail = &v.Snippet.Thumbnails.Maxres.Url
		listResultPlaylist.List = append(listResultPlaylist.List, playlist)
	}
	return &listResultPlaylist, nil
}

func convertPlaylistVideo(url string) (*ListResultPlaylistVideo, error) {
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary PlaylistVideoTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	listResultPlaylistVideo := ListResultPlaylistVideo{}
	listResultPlaylistVideo.Total = summary.PageInfo.TotalResults
	listResultPlaylistVideo.Limit = summary.PageInfo.ResultsPerPage
	listResultPlaylistVideo.NextPageToken = summary.NextPageToken
	for _, v := range summary.Items {
		playlistVideo := PlaylistVideo{}
		playlistVideo.Id = v.ContentDetails.VideoId
		playlistVideo.Title = v.Snippet.Title
		playlistVideo.Description = v.Snippet.Description
		playlistVideo.PublishedAt = &v.Snippet.PublishedAt
		playlistVideo.LocalizedTitle = v.Snippet.Localized.Title
		playlistVideo.LocalizedDescription = v.Snippet.Localized.Description
		playlistVideo.ChannelId = v.Snippet.ChannelId
		playlistVideo.ChannelTitle = v.Snippet.ChannelTitle
		playlistVideo.PlaylistId = v.Snippet.PlaylistId
		playlistVideo.Position = v.Snippet.Position
		playlistVideo.VideoOwnerChannelId = v.Snippet.VideoOwnerChannelId
		playlistVideo.VideoOwnerChannelTitle = v.Snippet.VideoOwnerChannelTitle
		playlistVideo.Thumbnail = &v.Snippet.Thumbnails.Default.Url
		playlistVideo.MediumThumbnail = &v.Snippet.Thumbnails.Medium.Url
		playlistVideo.HighThumbnail = &v.Snippet.Thumbnails.High.Url
		playlistVideo.StandardThumbnail = &v.Snippet.Thumbnails.Standard.Url
		playlistVideo.MaxresThumbnail = &v.Snippet.Thumbnails.Maxres.Url
		listResultPlaylistVideo.List = append(listResultPlaylistVideo.List, playlistVideo)
	}
	return &listResultPlaylistVideo, nil
}

func convertVideos(url string) (*ListResultVideos, error) {
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary VideoTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	//log.Println(string(body))
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	listResultVideos := ListResultVideos{}
	listResultVideos.Total = summary.PageInfo.TotalResults
	listResultVideos.Limit = summary.PageInfo.ResultsPerPage
	listResultVideos.NextPageToken = summary.NextPageToken
	for _, v := range summary.Items {
		video := Video{}
		video.Id = v.Id
		video.Title = v.Snippet.Title
		video.Description = v.Snippet.Description
		video.PublishedAt = &v.Snippet.PublishedAt
		video.LocalizedTitle = v.Snippet.Localized.Title
		video.LocalizedDescription = v.Snippet.Localized.Description
		video.ChannelId = v.Snippet.ChannelId
		video.ChannelTitle = v.Snippet.ChannelTitle
		video.Tags = v.Snippet.Tags
		video.CategoryId = v.Snippet.CategoryId
		video.LiveBroadcastContent = v.Snippet.LiveBroadcastContent
		video.DefaultLanguage = v.Snippet.DefaultLanguage
		video.DefaultAudioLanguage = v.Snippet.DefaultAudioLanguage
		duration, err := calculateDuration(v.ContentDetails.Duration)
		if err != nil {
			return nil, err
		}
		video.Duration = int64(duration)
		video.Dimension = v.ContentDetails.Dimension
		if v.ContentDetails.Definition == "hd" {
			video.Definition = 5
		} else {
			video.Definition = 4
		}
		video.Caption = v.ContentDetails.Caption
		video.LicensedContent = &v.ContentDetails.LicensedContent
		if v.ContentDetails.Projection == "rectangular" {
			video.Projection = ""
		} else {
			video.Projection = "3"
		}
		video.Thumbnail = &v.Snippet.Thumbnails.Default.Url
		video.HighThumbnail = &v.Snippet.Thumbnails.Medium.Url
		video.StandardThumbnail = &v.Snippet.Thumbnails.Standard.Url
		video.MaxresThumbnail = &v.Snippet.Thumbnails.Maxres.Url
		if len(v.ContentDetails.RegionRestriction.Allow) > 0 {
			video.AllowedRegions = v.ContentDetails.RegionRestriction.Allow
		}
		if len(v.ContentDetails.RegionRestriction.Blocked) > 0 {
			video.BlockedRegions = v.ContentDetails.RegionRestriction.Blocked
		}
		listResultVideos.List = append(listResultVideos.List, video)
	}
	return &listResultVideos, nil
}

func calculateDuration(d string) (float64, error) {
	if d == "" {
		return 0, nil
	}
	k := strings.Split(d, "M")
	if len(k) < 2 {
		g := strings.Split(d, "H")
		if len(g) < 2 {
			var a0 string
			if len(d) > 0 {
				a0 = d[2 : len(d)-1]
			}
			a1, er0 := strconv.ParseFloat(a0, 32)
			if er0 != nil {
				return -1, er0
			}
			if strings.Contains(d, "S") {
				return a1, nil
			} else {
				return a1 * 3600, nil
			}
		} else {
			var a0 string
			if len(d) > 0 {
				a0 = d[2 : len(d)-3]
			}
			a3, er0 := strconv.ParseFloat(a0[0:len(a0)-1], 32)
			if er0 != nil {
				return -1, er0
			}
			return a3 * 3600, nil
		}
	}
	var a string
	if k[1] != "" {
		a = k[1][0 : len(k[1])-1]
	}
	if len(a) == 0 {
		a = "0"
	}

	x := strings.Split(k[0], "H")
	var b string
	if len(x) == 1 && len(k[0]) > 0 {
		b = k[0][2:len(k[0])]
	} else {
		b = x[1]
	}
	a1, er0 := strconv.ParseFloat(a, 32)
	if er0 != nil {
		return -1, er0
	}
	a2, er1 := strconv.ParseFloat(b, 32)
	if er1 != nil {
		return -1, er1
	}
	if !math.IsNaN(a1) && !math.IsNaN(a2) {
		if len(x) == 1 {
			return a2*60 + a1, nil
		} else {
			var c string
			if len(x[0]) > 0 {
				c = x[0][2:len(x[0])]
			}
			c1, er2 := strconv.ParseFloat(c, 32)
			if er2 != nil {
				return -1, er2
			}
			if !math.IsNaN(c1) {
				return c1*3600 + a2*60 + a1, nil
			} else {
				return 0, nil
			}
		}
	}
	return 0, nil
}

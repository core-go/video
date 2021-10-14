package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"

	"github.com/core-go/video"
)

type VideoHandler struct {
	Video VideoService
	channelType reflect.Type
	playlistType reflect.Type
	videoType reflect.Type
	channelFields []string
	playlistFields []string
	videoFields []string
}

func NewVideoHandler(clientService VideoService) (*VideoHandler,error) {
	var channel Channel
	channelType := reflect.TypeOf(channel)
	if channelType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}

	var playlist Playlist
	playlistType := reflect.TypeOf(playlist)
	if playlistType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}

	var video Video
	videoType := reflect.TypeOf(video)
	if videoType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}

	channelFields := getFields(channelType)
	playlistFields := getFields(playlistType)
	videoFields := getFields(videoType)

	return &VideoHandler{
		Video: clientService,
		channelType: channelType,
		playlistType: playlistType,
		videoType: videoType,
		channelFields: channelFields,
		playlistFields: playlistFields,
		videoFields: videoFields,
	}, nil
}

func (c *VideoHandler) GetChannel(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	s := GetRequiredParam(w, r)
	if len(s) > 0 {
		fields := QueryArray(ps, "fields", c.channelFields)
		res, err := c.Video.GetChannel(r.Context(), s, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetChannels(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	arrayId := GetRequiredParams(w, r)
	if len(arrayId) > 0 {
		fields := QueryArray(ps, "fields", c.channelFields)
		res, err := c.Video.GetChannels(r.Context(), arrayId, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetPlaylist(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	s := GetRequiredParam(w, r)
	if len(s) > 0 {
		fields := QueryArray(ps, "fields", c.playlistFields)
		res, err := c.Video.GetPlaylist(r.Context(), s, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetPlaylists(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	arrayId := GetRequiredParams(w, r)
	if len(arrayId) > 0 {
		fields := QueryArray(ps, "fields", c.playlistFields)
		res, err := c.Video.GetPlaylists(r.Context(), arrayId, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetVideo(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	s := GetRequiredParam(w, r)
	if len(s) > 0 {
		fields := QueryArray(ps, "fields", c.videoFields)
		res, err := c.Video.GetVideo(r.Context(), s, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetVideos(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	arrayId := GetRequiredParams(w, r)
	if len(arrayId) > 0 {
		fields := QueryArray(ps, "fields", c.videoFields)
		res, err := c.Video.GetVideos(r.Context(), arrayId, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetChannelPlaylists(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	channelId := QueryRequiredString(w, query, "channelId")
	if len(channelId) > 0 {
		limit := QueryInt(query, "limit", 10)
		nextPageToken := QueryString(query, "nextPageToken")
		fields := QueryArray(query, "fields", c.playlistFields)
		res, err := c.Video.GetChannelPlaylists(r.Context(), channelId, *limit, nextPageToken, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetVideosFromChannelIdOrPlaylistId(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.videoFields)

	playlistId := query.Get("playlistId")
	if len(playlistId) > 0 {
		res, er1 := c.Video.GetPlaylistVideos(r.Context(), playlistId, *limit, nextPageToken, fields)
		if er1 != nil {
			http.Error(w, er1.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	} else {
		channelId := QueryRequiredString(w, query, "channelId")
		if len(channelId) > 0 {
			res, er1 := c.Video.GetChannelVideos(r.Context(), channelId, *limit, nextPageToken, fields)
			if er1 != nil {
				http.Error(w, er1.Error(), http.StatusInternalServerError)
				return
			}
			respond(w, res)
		}
	}
}

func (c *VideoHandler) GetCategory(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r)
	res, err := c.Video.GetCategories(r.Context(), s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchChannel(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.channelFields)

	var channelSM ChannelSM
	channelSM.Q = strings.TrimSpace(query.Get("q"))
	channelSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	channelSM.Sort = strings.TrimSpace(query.Get("sort"))
	channelSM.RegionCode = query.Get("regionCode")

	if query.Get("publishedAfter") != "" {
		channelSM.PublishedAfter = CreateTime(query.Get("publishedAfter"))
	}

	if query.Get("publishedBefore") != "" {
		channelSM.PublishedBefore = CreateTime(query.Get("publishedBefore"))
	}

	res, er1 := c.Video.SearchChannel(r.Context(), channelSM, *limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchPlaylists(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.playlistFields)

	var playlistSM PlaylistSM
	playlistSM.Q = strings.TrimSpace(query.Get("q"))
	playlistSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	playlistSM.Sort = strings.TrimSpace(query.Get("sort"))
	playlistSM.RegionCode = query.Get("regionCode")

	if query.Get("publishedAfter") != "" {
		playlistSM.PublishedAfter = CreateTime(query.Get("publishedAfter"))
	}

	if query.Get("publishedBefore") != "" {
		playlistSM.PublishedBefore = CreateTime(query.Get("publishedBefore"))
	}

	res, er1 := c.Video.SearchPlaylists(r.Context(), playlistSM, *limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.videoFields)

	var itemSM ItemSM
	itemSM.Q = strings.TrimSpace(query.Get("q"))
	itemSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	itemSM.Sort = strings.TrimSpace(query.Get("sort"))
	itemSM.RegionCode = query.Get("regionCode")

	if query.Get("publishedAfter") != "" {
		itemSM.PublishedAfter = CreateTime(query.Get("publishedAfter"))
	}

	if query.Get("publishedBefore") != "" {
		itemSM.PublishedBefore = CreateTime(query.Get("publishedBefore"))
	}

	if query.Get("duration") != "" {
		itemSM.Duration = query.Get("duration")
	}

	res, er1 := c.Video.SearchVideos(r.Context(), itemSM, *limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) Search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.videoFields)

	var itemSM ItemSM
	itemSM.Q = strings.TrimSpace(query.Get("q"))
	itemSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	itemSM.Sort = strings.TrimSpace(query.Get("sort"))
	itemSM.RegionCode = query.Get("regionCode")

	if query.Get("publishedAfter") != "" {
		itemSM.PublishedAfter = CreateTime(query.Get("publishedAfter"))
	}

	if query.Get("publishedBefore") != "" {
		itemSM.PublishedBefore = CreateTime(query.Get("publishedBefore"))
	}

	if query.Get("duration") != "" {
		itemSM.Duration = query.Get("duration")
	}

	res, er1 := c.Video.Search(r.Context(), itemSM, *limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetRelatedVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	id := QueryRequiredString(w, query, "id")
	if len(id) > 0 {
		limit := QueryInt(query, "limit", 10)
		nextPageToken := QueryString(query, "nextPageToken")
		fields := QueryArray(query, "fields", c.videoFields)
		res, err := c.Video.GetRelatedVideos(r.Context(), id, *limit, nextPageToken, fields)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetPopularVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	categoryId := query.Get("categoryId")
	regionCode := query.Get("regionCode")

	limit := QueryInt(query, "limit", 10)
	nextPageToken := QueryString(query, "nextPageToken")
	fields := QueryArray(query, "fields", c.videoFields)
	res, err := c.Video.GetPopularVideos(r.Context(), regionCode, categoryId, *limit, nextPageToken, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func getFields(modelType reflect.Type) (res []string) {
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		jsonTag := field.Tag.Get("json")
		jsonField := strings.Split(jsonTag, ",")[0]
		if len(jsonTag) > 0 {
			res = append(res, jsonField)
		}
	}
	return res
}

func respond(w http.ResponseWriter, result interface{}) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/core-go/video"
)

type VideoHandler struct {
	Video video.VideoService
	channelType reflect.Type
	playlistType reflect.Type
	videoType reflect.Type
}

func NewVideoHandler(clientService video.VideoService) (*VideoHandler,error) {
	var channel video.Channel
	channelType := reflect.TypeOf(channel)
	if channelType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}
	var playlist video.Playlist
	playlistType := reflect.TypeOf(playlist)
	if playlistType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}
	var video video.Video
	videoType := reflect.TypeOf(video)
	if videoType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}
	return &VideoHandler{
		Video: clientService,
		channelType: channelType,
		playlistType: playlistType,
		videoType: videoType,
	}, nil
}

func (c *VideoHandler) GetChannel(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "?")
	if len(s) <= 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.channelType, fields)
	res, err := c.Video.GetChannel(r.Context(), s, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetChannels(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "&")
	if len(s) <= 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	arrayId := strings.Split(s, ",")
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.channelType, fields)
	res, err := c.Video.GetChannels(r.Context(), arrayId, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetPlaylist(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "&")
	if len(s) <= 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.playlistType, fields)
	res, err := c.Video.GetPlaylist(r.Context(), s, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetPlaylists(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "&")
	if len(s) <= 0 {
		http.Error(w, "id cannot be empty", http.StatusBadRequest)
		return
	}
	ids := strings.Split(s, ",")
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.playlistType, fields)
	res, err := c.Video.GetPlaylists(r.Context(), ids, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	respond(w, res)
}

func (c *VideoHandler) GetVideo(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "&")
	if len(s) <= 0 {
		http.Error(w, "Id cannot empty!", http.StatusBadRequest)
		return
	}
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.videoType, fields)
	res, err := c.Video.GetVideo(r.Context(), s, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetVideos(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//s := strings.Split(params, "&")
	if len(s) <= 0 {
		http.Error(w, "Ids cannot be empty!", http.StatusBadRequest)
		return
	}
	ids := strings.Split(s, ",")
	var fields []string
	ps := r.URL.Query()
	fieldsEle := ps.Get("fields")
	if len(fieldsEle) > 0 {
		fields = strings.Split(fieldsEle, ",")
	}
	fields = checkFields(c.videoType, fields)
	res, err := c.Video.GetVideos(r.Context(), ids, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetChannelPlaylists(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	channelId := query.Get("channelId")
	if len(channelId) <= 0 {
		http.Error(w, "ChannelId cannot be empty", http.StatusBadRequest)
		return
	}
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.playlistType, fields)
	res, er1 := c.Video.GetChannelPlaylists(r.Context(), channelId, limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetVideosFromChannelIdOrPlaylistId(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	playlistId := query.Get("playlistId")
	if len(playlistId) > 0 {
		limitString := query.Get("limit")
		var limit int
		if len(limitString) > 0 {
			res, err := strconv.Atoi(limitString)
			if err != nil {
				http.Error(w, "Limit is not number", http.StatusBadRequest)
			}
			limit = res
		} else {
			limit = 10
		}
		nextPageToken := query.Get("nextPageToken")
		if len(nextPageToken) <= 0 {
			nextPageToken = ""
		}
		var fields []string
		fieldsString := query.Get("fields")
		if len(fieldsString) > 0 {
			fields = strings.Split(fieldsString, ",")
		}
		fields = checkFields(c.videoType, fields)
		res, er1 := c.Video.GetPlaylistVideos(r.Context(), playlistId, limit, nextPageToken, fields)
		if er1 != nil {
			http.Error(w, er1.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	} else {
		channelId := query.Get("channelId")
		if len(channelId) <= 0 {
			http.Error(w, "Require channelId or playlistId", http.StatusBadRequest)
			return
		}
		limitString := query.Get("limit")
		var limit int
		if len(limitString) > 0 {
			res, err := strconv.Atoi(limitString)
			if err != nil {
				http.Error(w, "Limit is not number", http.StatusBadRequest)
			}
			limit = res
		} else {
			limit = 10
		}
		nextPageToken := query.Get("nextPageToken")
		if len(nextPageToken) <= 0 {
			nextPageToken = ""
		}
		var fields []string
		fieldsString := query.Get("fields")
		if len(fieldsString) > 0 {
			fields = strings.Split(fieldsString, ",")
		}
		fields = checkFields(c.videoType, fields)
		res, er1 := c.Video.GetChannelVideos(r.Context(), channelId, limit, nextPageToken, fields)
		if er1 != nil {
			http.Error(w, er1.Error(), http.StatusInternalServerError)
			return
		}
		respond(w, res)
	}
}

func (c *VideoHandler) GetCategory(w http.ResponseWriter, r *http.Request) {
	s := GetParam(r, 0)
	//params := mux.Vars(r)["params"]
	res, err := c.Video.GetCategories(r.Context(), s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchChannel(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.channelType, fields)
	var channelSM video.ChannelSM
	channelSM.Q = strings.TrimSpace(query.Get("q"))
	channelSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	channelSM.Sort = strings.TrimSpace(query.Get("sort"))
	layout := "2006-01-02T15:04:05Z"
	if query.Get("publishedAfter") != "" {
		t, err := time.Parse(layout, query.Get("publishedAfter"))
		if err != nil {
			http.Error(w, "publishedAfter is not time", http.StatusBadRequest)
			return
		}
		channelSM.PublishedAfter = &t
	}

	if query.Get("publishedBefore") != "" {
		t1, err := time.Parse(layout, query.Get("publishedBefore"))
		if err != nil {
			http.Error(w, "publishedBefore is not time", http.StatusBadRequest)
			return
		}
		channelSM.PublishedBefore = &t1
	}

	channelSM.RegionCode = query.Get("regionCode")
	res, er1 := c.Video.SearchChannel(r.Context(), channelSM, limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchPlaylists(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.playlistType, fields)
	var playlistSM video.PlaylistSM
	playlistSM.Q = strings.TrimSpace(query.Get("q"))
	playlistSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	playlistSM.Sort = strings.TrimSpace(query.Get("sort"))
	layout := "2006-01-02T15:04:05Z"
	if query.Get("publishedAfter") != "" {
		t, err := time.Parse(layout, query.Get("publishedAfter"))
		if err != nil {
			http.Error(w, "publishedAfter is not time", http.StatusBadRequest)
			return
		}
		playlistSM.PublishedAfter = &t
	}

	if query.Get("publishedBefore") != "" {
		t1, err := time.Parse(layout, query.Get("publishedBefore"))
		if err != nil {
			http.Error(w, "publishedBefore is not time", http.StatusBadRequest)
			return
		}
		playlistSM.PublishedBefore = &t1
	}

	playlistSM.RegionCode = query.Get("regionCode")
	res, er1 := c.Video.SearchPlaylists(r.Context(), playlistSM, limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) SearchVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.videoType, fields)
	var itemSM video.ItemSM
	itemSM.Q = strings.TrimSpace(query.Get("q"))
	itemSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	itemSM.Sort = strings.TrimSpace(query.Get("sort"))
	layout := "2006-01-02T15:04:05Z"
	if query.Get("publishedAfter") != "" {
		t, err := time.Parse(layout, query.Get("publishedAfter"))
		if err != nil {
			http.Error(w, "publishedAfter is not time", http.StatusBadRequest)
			return
		}
		itemSM.PublishedAfter = &t
	}

	if query.Get("publishedBefore") != "" {
		t1, err := time.Parse(layout, query.Get("publishedBefore"))
		if err != nil {
			http.Error(w, "publishedBefore is not time", http.StatusBadRequest)
			return
		}
		itemSM.PublishedBefore = &t1
	}
	if query.Get("duration") != "" {
		itemSM.Duration = query.Get("duration")
	}
	itemSM.RegionCode = query.Get("regionCode")
	res, er1 := c.Video.SearchVideos(r.Context(), itemSM, limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) Search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.videoType,fields)
	var itemSM video.ItemSM
	itemSM.Q = strings.TrimSpace(query.Get("q"))
	itemSM.ChannelId = strings.TrimSpace(query.Get("channelId"))
	itemSM.Sort = strings.TrimSpace(query.Get("sort"))
	layout := "2006-01-02T15:04:05Z"
	if query.Get("publishedAfter") != "" {
		t, err := time.Parse(layout, query.Get("publishedAfter"))
		if err != nil {
			http.Error(w, "publishedAfter is not time", http.StatusBadRequest)
			return
		}
		itemSM.PublishedAfter = &t
	}

	if query.Get("publishedBefore") != "" {
		t1, err := time.Parse(layout, query.Get("publishedBefore"))
		if err != nil {
			http.Error(w, "publishedBefore is not time", http.StatusBadRequest)
			return
		}
		itemSM.PublishedBefore = &t1
	}
	if query.Get("duration") != "" {
		itemSM.Duration = query.Get("duration")
	}
	itemSM.RegionCode = query.Get("regionCode")
	res, er1 := c.Video.Search(r.Context(), itemSM, limit, nextPageToken, fields)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetRelatedVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	id := query.Get("id")
	if len(id) <= 0 {
		http.Error(w, "id can not empty", http.StatusBadRequest)
	}
	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.videoType, fields)
	res, err := c.Video.GetRelatedVideos(r.Context(), id, limit, nextPageToken, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func (c *VideoHandler) GetPopularVideos(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	categoryId := query.Get("categoryId")
	regionCode := query.Get("regionCode")

	limitString := query.Get("limit")
	var limit int
	if len(limitString) > 0 {
		res, err := strconv.Atoi(limitString)
		if err != nil {
			http.Error(w, "Limit is not number", http.StatusBadRequest)
		}
		limit = res
	} else {
		limit = 10
	}
	nextPageToken := query.Get("nextPageToken")
	if len(nextPageToken) <= 0 {
		nextPageToken = ""
	}
	var fields []string
	fieldsString := query.Get("fields")
	if len(fieldsString) > 0 {
		fields = strings.Split(fieldsString, ",")
	}
	fields = checkFields(c.videoType, fields)
	res, err := c.Video.GetPopularVideos(r.Context(), regionCode, categoryId, limit, nextPageToken, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, res)
}

func checkFields(modelType reflect.Type, fields []string) (res []string) {
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("json")
		jsonField := strings.Split(ormTag, ",")[0]
		if len(ormTag) > 0 {
			for _, field := range fields {
				if strings.TrimSpace(field) == jsonField {
					res = append(res, jsonField)
					break
				}
			}
		}
	}
	return res
}

func respond(w http.ResponseWriter, result interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(result)
	return err
}

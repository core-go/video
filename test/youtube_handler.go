package test

import (
	"encoding/json"
	. "github.com/core-go/video"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type YoutubeHandler struct {
	service SyncClient
}

func NewTubeHandler(syncClient SyncClient) *YoutubeHandler {
	return &YoutubeHandler{service: syncClient}
}

func (t *YoutubeHandler) GetChannel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	result, err := t.service.GetChannel(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetChannels(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	s := strings.Split(id, ",")
	result, err := t.service.GetChannels(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetPlaylist(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	result, err := t.service.GetPlaylist(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetPlaylists(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	s := strings.Split(id, ",")
	result, err := t.service.GetPlaylists(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetChannelPlaylists(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	s := strings.Split(id, ",")
	log.Println(s)
	var channelId string
	if s[0] != "" {
		channelId = s[0]
	}
	var max int64
	if s[1] != "" {
		maxR, err := strconv.ParseInt(s[1], 10, 64)
		if err != nil {
			http.Error(w, "cannot be empty", http.StatusBadRequest)
			return
		}
		max = maxR
	}
	var nextPageToken string
	if s[2] != "" {
		nextPageToken = s[2]
	}
	result, err := t.service.GetChannelPlaylists(channelId, int16(max), nextPageToken)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetPlaylistVideos(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	s := strings.Split(id, ",")
	log.Println(s)
	var channelId string
	if s[0] != "" {
		channelId = s[0]
	}
	var max int64
	if s[1] != "" {
		maxR, err := strconv.ParseInt(s[1], 10, 64)
		if err != nil {
			http.Error(w, "cannot be empty", http.StatusBadRequest)
			return
		}
		max = maxR
	}
	var nextPageToken string
	if s[2] != "" {
		nextPageToken = s[2]
	}
	result, err := t.service.GetPlaylistVideos(channelId, int16(max), nextPageToken)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func (t *YoutubeHandler) GetVideos(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Id cannot be empty", http.StatusBadRequest)
		return
	}
	s := strings.Split(id, ",")
	result, err := t.service.GetVideos(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respond(w, result)
}

func respond(w http.ResponseWriter, result interface{}) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

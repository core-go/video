package sync

import (
	"encoding/json"
	"fmt"
	. "github.com/core-go/video"
	"github.com/gorilla/mux"
	"net/http"
)

type SyncHandler struct {
	sync SyncService
}

type ChannelId struct {
	ChannelId string `json:"channelId,omitempty"`
	Level     int    `json:"level,omitempty"`
}

type PlaylistId struct {
	PlaylistId string `json:"playlistId,omitempty"`
	Level      int    `json:"level,omitempty"`
}

func NewSyncHandler(syncService SyncService) *SyncHandler {
	return &SyncHandler{sync: syncService}
}

func (h *SyncHandler) SyncChannel(w http.ResponseWriter, r *http.Request) {
	var channelId ChannelId
	er1 := json.NewDecoder(r.Body).Decode(&channelId)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusBadRequest)
		return
	}
	resultChannel, er2 := h.sync.SyncChannel(r.Context(), channelId.ChannelId)
	if er2 != nil {
		http.Error(w, er2.Error(), http.StatusBadRequest)
		return
	}
	result := ""
	if resultChannel > 0 {
		result = fmt.Sprintf(`Sync %d channel successfully`, resultChannel)
	} else {
		result = "Invalid channel to sync"
	}
	respond(w, result)
}

func (h *SyncHandler) SyncPlaylist(w http.ResponseWriter, r *http.Request) {
	var playlistId PlaylistId
	er1 := json.NewDecoder(r.Body).Decode(&playlistId)
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusBadRequest)
		return
	}
	resultChannel, er2 := h.sync.SyncPlaylist(r.Context(), playlistId.PlaylistId, &playlistId.Level)
	if er2 != nil {
		http.Error(w, er2.Error(), http.StatusBadRequest)
		return
	}
	result := ""
	if resultChannel > 0 {
		result = fmt.Sprintf("Sync playlist successfully")
	} else {
		result = "Invalid playlist to sync"
	}
	respond(w, result)
}

func (h *SyncHandler) SyncSubctiption(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if len(id) <= 0 {
		http.Error(w, "Id cannot empty", http.StatusBadRequest)
		return
	}
	resultChannel, er2 := h.sync.GetSubscriptions(r.Context(), id)
	if er2 != nil {
		http.Error(w, er2.Error(), http.StatusBadRequest)
		return
	}
	respond(w, resultChannel)
}

func respond(w http.ResponseWriter, result interface{}) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

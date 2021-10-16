package mux

import (
	"context"
	"github.com/gorilla/mux"
	"net/http"
)

const (
	GET    = "GET"
	POST   = "POST"
	PUT    = "PUT"
	DELETE = "DELETE"
)

type Sync interface {
	SyncChannel(w http.ResponseWriter, r *http.Request)
	SyncPlaylist(w http.ResponseWriter, r *http.Request)
	SyncSubscription(w http.ResponseWriter, r *http.Request)
}

type Service interface {
	GetChannel(w http.ResponseWriter, r *http.Request)
	GetChannels(w http.ResponseWriter, r *http.Request)
	GetPlaylist(w http.ResponseWriter, r *http.Request)
	GetPlaylists(w http.ResponseWriter, r *http.Request)
	GetVideo(w http.ResponseWriter, r *http.Request)
	GetVideos(w http.ResponseWriter, r *http.Request)
	GetChannelPlaylists(w http.ResponseWriter, r *http.Request)
	GetVideosFromChannelIdOrPlaylistId(w http.ResponseWriter, r *http.Request)
	GetCategory(w http.ResponseWriter, r *http.Request)
	SearchChannel(w http.ResponseWriter, r *http.Request)
	SearchPlaylists(w http.ResponseWriter, r *http.Request)
	SearchVideos(w http.ResponseWriter, r *http.Request)
	GetRelatedVideos(w http.ResponseWriter, r *http.Request)
	GetPopularVideos(w http.ResponseWriter, r *http.Request)
	Search(w http.ResponseWriter, r *http.Request)
}

func Register(ctx context.Context, r *mux.Router, param string, service Service)  {
	s := r.PathPrefix(param).Subrouter()
	s.HandleFunc("/category", service.GetCategory).Methods(GET)
	s.HandleFunc("/channels/search", service.SearchChannel).Methods(GET)
	s.HandleFunc("/channels/list", service.GetChannels).Methods(GET)
	s.HandleFunc("/channels/{id}", service.GetChannel).Methods(GET)
	s.HandleFunc("/playlists/search", service.SearchPlaylists).Methods(GET)
	s.HandleFunc("/playlists/list", service.GetPlaylists).Methods(GET)
	s.HandleFunc("/playlists", service.GetChannelPlaylists).Methods(GET)
	s.HandleFunc("/playlists/{id}", service.GetPlaylist).Methods(GET)
	s.HandleFunc("/videos/popular", service.GetPopularVideos).Methods(GET)
	s.HandleFunc("/videos/search", service.SearchVideos).Methods(GET)
	s.HandleFunc("/videos/list", service.GetVideos).Methods(GET)
	s.HandleFunc("/video/{id}", service.GetVideo).Methods(GET)
	s.HandleFunc("/videos/{id}/related", service.GetRelatedVideos).Methods(GET)
	s.HandleFunc("/videos", service.GetVideosFromChannelIdOrPlaylistId).Methods(GET)
	s.HandleFunc("/search", service.Search).Methods(GET)
}

func RegisterSync(ctx context.Context, r *mux.Router, param string, sync Sync)  {
	s := r.PathPrefix(param).Subrouter()
	s.HandleFunc("/channel", sync.SyncChannel).Methods(POST)
	s.HandleFunc("/playlists", sync.SyncPlaylist).Methods(POST)
	s.HandleFunc("/channels/subscriptions/{id}", sync.SyncSubscription).Methods(GET)
}

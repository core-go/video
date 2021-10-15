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

type AppSync interface {
	SyncChannel(w http.ResponseWriter, r *http.Request)
	SyncPlaylist(w http.ResponseWriter, r *http.Request)
	SyncSubscription(w http.ResponseWriter, r *http.Request)
}

type AppClient interface {
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

func RegisterRoot(ctx context.Context, r *mux.Router, param string, appSync AppSync, appClient AppClient)  {
	s := r.PathPrefix(param).Subrouter()
	s.HandleFunc("/channel", appSync.SyncChannel).Methods(POST)
	s.HandleFunc("/playlists", appSync.SyncPlaylist).Methods(POST)
	s.HandleFunc("/channels/subscriptions/{id}", appSync.SyncSubscription).Methods(GET)

	s.HandleFunc("/category", appClient.GetCategory).Methods(GET)
	s.HandleFunc("/channels/search", appClient.SearchChannel).Methods(GET)
	s.HandleFunc("/channels/list", appClient.GetChannels).Methods(GET)
	s.HandleFunc("/channels/{id}", appClient.GetChannel).Methods(GET)
	s.HandleFunc("/playlists/search", appClient.SearchPlaylists).Methods(GET)
	s.HandleFunc("/playlists/list", appClient.GetPlaylists).Methods(GET)
	s.HandleFunc("/playlists", appClient.GetChannelPlaylists).Methods(GET)
	s.HandleFunc("/playlists/{id}", appClient.GetPlaylist).Methods(GET)
	s.HandleFunc("/videos/popular", appClient.GetPopularVideos).Methods(GET)
	s.HandleFunc("/videos/search", appClient.SearchVideos).Methods(GET)
	s.HandleFunc("/videos/list", appClient.GetVideos).Methods(GET)
	s.HandleFunc("/video/{id}", appClient.GetVideo).Methods(GET)
	s.HandleFunc("/videos/{id}/related", appClient.GetRelatedVideos).Methods(GET)
	s.HandleFunc("/videos", appClient.GetVideosFromChannelIdOrPlaylistId).Methods(GET)
	s.HandleFunc("/search", appClient.Search).Methods(GET)
}
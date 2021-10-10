package mongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/core-go/video"
	"github.com/core-go/video/category"
)

type MongoVideoService struct {
	ChannelCollection       *mongo.Collection
	ChannelSyncCollection   *mongo.Collection
	PlaylistCollection      *mongo.Collection
	PlaylistVideoCollection *mongo.Collection
	VideoCollection         *mongo.Collection
	CategoryCollection      *mongo.Collection
	TubeCategory            category.CategorySyncClient
}

func NewMongoVideoService(db *mongo.Database, channelCollectionName string, channelSyncCollectionName string, playlistCollectionName string, playlistVideoCollectionName string, videoCollectionName string, categoryCollection string, TubeCategory category.CategorySyncClient) *MongoVideoService {
	return &MongoVideoService{
		ChannelCollection:       db.Collection(channelCollectionName),
		ChannelSyncCollection:   db.Collection(channelSyncCollectionName),
		PlaylistCollection:      db.Collection(playlistCollectionName),
		PlaylistVideoCollection: db.Collection(playlistVideoCollectionName),
		VideoCollection:         db.Collection(videoCollectionName),
		CategoryCollection:      db.Collection(categoryCollection),
		TubeCategory:            TubeCategory,
	}
}

func (m *MongoVideoService) GetChannel(ctx context.Context, channelId string, fields []string) (*video.Channel, error) {
	query := bson.M{"_id": channelId}
	optionsFind := options.FindOne()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	result := m.ChannelCollection.FindOne(ctx, query, optionsFind)
	if result.Err() != nil {
		if strings.Contains(result.Err().Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, result.Err()
	}
	var res video.Channel
	er1 := result.Decode(&res)
	if er1 != nil {
		return nil, er1
	}
	if len(res.ChannelList) > 0 {
		channels, err := m.GetChannels(ctx, res.ChannelList, []string{});
		if err != nil {
			return nil, err
		}
		res.Channels = *channels
	}
	return &res, nil
}

func (m *MongoVideoService) GetChannels(ctx context.Context, ids []string, fields []string) (*[]video.Channel, error) {
	query := bson.M{"_id": bson.M{"$in": ids}}
	optionsFind := options.Find()
	if len(ids) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	result, er0 := m.ChannelCollection.Find(ctx, query, optionsFind)
	if er0 != nil {
		if strings.Contains(er0.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, er0
	}
	var res []video.Channel
	defer result.Close(ctx)
	for result.Next(ctx) {
		var channel video.Channel
		if err := result.Decode(&channel); err != nil {
			return nil, err
		}
		res = append(res, channel)
	}
	return &res, nil
}

func (m *MongoVideoService) GetPlaylist(ctx context.Context, id string, fields []string) (*video.Playlist, error) {
	query := bson.M{"_id": id}
	optionsFindOne := options.FindOne()
	if len(fields) > 0 {
		optionsFindOne.SetProjection(sel(fields...))
	}
	res := m.PlaylistCollection.FindOne(ctx, query, optionsFindOne)
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, res.Err()
	}
	var playlist video.Playlist
	err := res.Decode(&playlist)
	if err != nil {
		return nil, err
	}
	return &playlist, err
}

func (m *MongoVideoService) GetPlaylists(ctx context.Context, ids []string, fields []string) (*[]video.Playlist, error) {
	query := bson.M{"_id": bson.M{"$in": ids}}
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	res, er0 := m.PlaylistCollection.Find(ctx, query, optionsFind)
	if er0 != nil {
		if strings.Contains(er0.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, er0
	}
	var result []video.Playlist
	defer res.Close(ctx)
	for res.Next(ctx) {
		var playlist video.Playlist
		if err := res.Decode(&playlist); err != nil {
			return nil, err
		}
		result = append(result, playlist)
	}
	return &result, nil
}

func (m *MongoVideoService) GetVideo(ctx context.Context, id string, fields []string) (*video.Video, error) {
	query := bson.M{"_id": id}
	optionsFindOne := options.FindOne()
	if len(fields) > 0 {
		optionsFindOne.SetProjection(sel(fields...))
	}
	res := m.VideoCollection.FindOne(ctx, query, optionsFindOne)
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, res.Err()
	}
	var video video.Video
	err := res.Decode(&video)
	if err != nil {
		return nil, err
	}
	return &video, err
}

func (m *MongoVideoService) GetVideos(ctx context.Context, ids []string, fields []string) (*[]video.Video, error) {
	query := bson.M{"_id": bson.M{"$in": ids}}
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	res, err := m.VideoCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	defer res.Close(ctx)
	var result []video.Video
	for res.Next(ctx) {
		var video video.Video
		if er1 := res.Decode(&video); er1 != nil {
			return nil, er1
		}
		result = append(result, video)
	}
	return &result, nil
}

func (m *MongoVideoService) GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	limit := getLimit(max)
	query := bson.M{"channelId": channelId, "count": bson.M{"$gt": 0}}
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	optionsFind.SetSort(bson.M{"publishedAt": 1})
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, er1 := m.PlaylistCollection.Find(ctx, query, optionsFind)
	if er1 != nil {
		if strings.Contains(er1.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, er1
	}
	var result video.ListResultPlaylist
	for res.Next(ctx) {
		var playlist video.Playlist
		if er2 := res.Decode(&playlist); er2 != nil {
			if strings.Contains(er2.Error(), "cannot decode invalid into") {
				break
			} else {
				return nil, er2
			}
		}
		result.List = append(result.List, playlist)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, nil
}

func (m *MongoVideoService) GetChannelVideos(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	query := bson.M{"channelId": channelId}
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	optionsFind.SetSort(bson.M{"publishedAt": 1})
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, err := m.VideoCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	var result video.ListResultVideos
	for res.Next(ctx) {
		var video video.Video
		if er1 := res.Decode(&video); er1 != nil {
			return nil, err
		}
		result.List = append(result.List, video)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, nil
}

func (m *MongoVideoService) GetPlaylistVideos(ctx context.Context, playlistId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := bson.M{"_id": playlistId}
	playlist := m.PlaylistVideoCollection.FindOne(ctx, query)
	if playlist.Err() != nil {
		if strings.Contains(playlist.Err().Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, playlist.Err()
	}
	var playlistVideo video.PlaylistVideoIdVideos
	er1 := playlist.Decode(&playlistVideo)
	if er1 != nil {
		return nil, er1
	}
	queryVideos := bson.M{"_id": bson.M{"$in": playlistVideo.Videos}}
	optionsVideos := options.Find()
	if len(fields) > 0 {
		optionsVideos = optionsVideos.SetProjection(sel(fields...))
	}
	optionsVideos.SetSkip(int64(*skip))
	optionsVideos.SetLimit(int64(limit))
	res, er2 := m.VideoCollection.Find(ctx, queryVideos, optionsVideos)
	if er2 != nil {
		if strings.Contains(er2.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, er2
	}
	var result video.ListResultVideos
	for res.Next(ctx) {
		var video video.Video
		if er3 := res.Decode(&video); er3 != nil {
			return nil, er3
		}
		result.List = append(result.List, video)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, nil
}

func (m *MongoVideoService) GetCategories(ctx context.Context, regionCode string) (*video.Categories, error) {
	query := bson.M{"_id": regionCode}
	res := m.CategoryCollection.FindOne(ctx, query)
	var category video.Categories
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "mongo: no documents in result") {
			res, er1 := m.TubeCategory.GetCagetories(regionCode)
			if er1 != nil {
				return nil, er1
			}
			category.Id = regionCode
			category.Data = *res
			_, er2 := m.CategoryCollection.InsertOne(ctx, category)
			if er2 != nil {
				return nil, er2
			}
			return &category, nil
		} else {
			return nil, res.Err()
		}
	}
	er0 := res.Decode(&category)
	if er0 != nil {
		return nil, er0
	}
	return &category, nil
}

func (m *MongoVideoService) SearchChannel(ctx context.Context, channelSM video.ChannelSM, max int, nextPageToken string, fields []string) (*video.ListResultChannel, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := buildQueryChannelSearch(channelSM)
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	if channelSM.Sort != "" {
		optionsFind.SetSort(bson.M{fmt.Sprintf(`%s`, channelSM.Sort): -1})
	}
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, err := m.ChannelCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	result := video.ListResultChannel{}
	for res.Next(ctx) {
		var channel video.Channel
		if er1 := res.Decode(&channel); er1 != nil {
			return nil, er1
		}
		result.List = append(result.List, channel)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, err
}

func (m *MongoVideoService) SearchPlaylists(ctx context.Context, playlistSM video.PlaylistSM, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := buildQueryPlaylistSearch(playlistSM)
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	if playlistSM.Sort != "" {
		optionsFind.SetSort(bson.M{fmt.Sprintf(`%s`, playlistSM.Sort): -1})
	}
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, err := m.PlaylistCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	result := video.ListResultPlaylist{}
	for res.Next(ctx) {
		var playlist video.Playlist
		if er1 := res.Decode(&playlist); er1 != nil {
			return nil, er1
		}
		result.List = append(result.List, playlist)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, err
}

func (m *MongoVideoService) SearchVideos(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := buildQueryVideoSearch(itemSM)
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	if itemSM.Sort != "" {
		optionsFind.SetSort(bson.M{fmt.Sprintf(`%s`, itemSM.Sort): -1})
	}
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, err := m.VideoCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	result := video.ListResultVideos{}
	for res.Next(ctx) {
		var video video.Video
		if er1 := res.Decode(&video); er1 != nil {
			return nil, er1
		}
		result.List = append(result.List, video)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, err
}

func (m *MongoVideoService) Search(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := buildQueryVideoSearch(itemSM)
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	if itemSM.Sort != "" {
		optionsFind.SetSort(bson.M{fmt.Sprintf(`%s`, itemSM.Sort): -1})
	}
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetLimit(int64(limit))
	res, err := m.VideoCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	result := video.ListResultVideos{}
	for res.Next(ctx) {
		var video video.Video
		if er1 := res.Decode(&video); er1 != nil {
			return nil, er1
		}
		result.List = append(result.List, video)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, err
}

func (m *MongoVideoService) GetRelatedVideos(ctx context.Context, videoId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	var a []string
	resVd, err := m.GetVideo(ctx, videoId, a)
	if err != nil {
		return nil, err
	}
	var result video.ListResultVideos
	if resVd == nil {
		return nil, errors.New("video don't exist")
	} else {
		array := []string{videoId}
		query := bson.M{"tags": bson.M{"$in": resVd.Tags}, "_id": bson.M{"$nin": array}}
		optionsFind := options.Find()
		if len(fields) > 0 {
			optionsFind.SetProjection(sel(fields...))
		}
		optionsFind.SetLimit(int64(limit))
		optionsFind.SetSkip(int64(*skip))
		optionsFind.SetSort(bson.M{"publishedAt": -1})
		res, err := m.VideoCollection.Find(ctx, query, optionsFind)
		if err != nil {
			if strings.Contains(err.Error(), "mongo: no documents in result") {
				return nil, nil
			}
			return nil, err
		}
		for res.Next(ctx) {
			var video video.Video
			if er1 := res.Decode(&video); er1 != nil {
				return nil, er1
			}
			result.List = append(result.List, video)
		}
		if len(result.List) > 0 {
			result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
		}
	}
	return &result, nil
}

func (m *MongoVideoService) GetPopularVideos(ctx context.Context, regionCode string, categoryId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	limit := getLimit(max)
	skip, er0 := getSkip(nextPageToken)
	if er0 != nil {
		return nil, er0
	}
	query := bson.D{}
	if regionCode != "" {
		query = append(query, bson.E{"blockedRegions", bson.M{"$ne": regionCode}})
	}
	if categoryId != "" {
		query = append(query, bson.E{"categoryId", categoryId})
	}
	optionsFind := options.Find()
	if len(fields) > 0 {
		optionsFind.SetProjection(sel(fields...))
	}
	optionsFind.SetLimit(int64(limit))
	optionsFind.SetSkip(int64(*skip))
	optionsFind.SetSort(bson.M{"publishedAt": -1})
	res, err := m.VideoCollection.Find(ctx, query, optionsFind)
	if err != nil {
		if strings.Contains(err.Error(), "mongo: no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	var result video.ListResultVideos
	for res.Next(ctx) {
		var video video.Video
		if er1 := res.Decode(&video); er1 != nil {
			return nil, er1
		}
		result.List = append(result.List, video)
	}
	if len(result.List) > 0 {
		result.NextPageToken = getNextPageToken(result.List[len(result.List)-1].Id, len(result.List), limit, *skip)
	}
	return &result, nil
}

func buildQueryChannelSearch(channelSM video.ChannelSM) bson.D {
	query := bson.D{}
	if channelSM.Q != "" {
		query = append(query, bson.E{"$or", []bson.M{{"title": primitive.Regex{Pattern: channelSM.Q, Options: "i"}}, {"description": primitive.Regex{Pattern: channelSM.Q, Options: "i"}}}})
	}
	if channelSM.PublishedBefore != nil && channelSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": channelSM.PublishedBefore, "$lte": channelSM.PublishedAfter}})
	} else if channelSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$lte": channelSM.PublishedAfter}})
	} else if channelSM.PublishedBefore != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": channelSM.PublishedBefore}})
	}
	if channelSM.ChannelId != "" {
		query = append(query, bson.E{"_id", channelSM.ChannelId})
	}
	if channelSM.ChannelType != "" {
		query = append(query, bson.E{"channelType", channelSM.ChannelType})
	}
	if channelSM.TopicId != "" {
		query = append(query, bson.E{"topicId", channelSM.TopicId})
	}
	if channelSM.RegionCode != "" {
		query = append(query, bson.E{"country", channelSM.RegionCode})
	}
	if channelSM.RelevanceLanguage != "" {
		query = append(query, bson.E{"relevanceLanguage", channelSM.RelevanceLanguage})
	}
	return query
}

func buildQueryPlaylistSearch(playlistSM video.PlaylistSM) bson.D {
	query := bson.D{}
	if playlistSM.Q != "" {
		query = append(query, bson.E{"$or", []bson.M{{"title": primitive.Regex{Pattern: playlistSM.Q, Options: "i"}}, {"description": primitive.Regex{Pattern: playlistSM.Q, Options: "i"}}}})
	}
	if playlistSM.PublishedBefore != nil && playlistSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": playlistSM.PublishedBefore, "$lte": playlistSM.PublishedAfter}})
	} else if playlistSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$lte": playlistSM.PublishedAfter}})
	} else if playlistSM.PublishedBefore != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": playlistSM.PublishedBefore}})
	}
	if playlistSM.ChannelId != "" {
		query = append(query, bson.E{"channelId", playlistSM.ChannelId})
	}
	if playlistSM.ChannelType != "" {
		query = append(query, bson.E{"channelType", playlistSM.ChannelType})
	}
	if playlistSM.RegionCode != "" {
		query = append(query, bson.E{"country", playlistSM.RegionCode})
	}
	if playlistSM.RelevanceLanguage != "" {
		query = append(query, bson.E{"relevanceLanguage", playlistSM.RelevanceLanguage})
	}
	return query
}

func buildQueryVideoSearch(itemSM video.ItemSM) bson.D {
	query := bson.D{}
	if itemSM.Duration != "" {
		switch itemSM.Duration {
		case "short":
			query = append(query, bson.E{"duration", bson.M{"$lte": 240}})
			break
		case "medium":
			query = append(query, bson.E{"duration", bson.M{"$gt": 240, "$lte": 1200}})
			break
		case "long":
			query = append(query, bson.E{"duration", bson.M{"$gt": 1200}})
			break
		default:
			break
		}
	}
	if itemSM.Q != "" {
		query = append(query, bson.E{"$or", []bson.M{{"title": primitive.Regex{Pattern: itemSM.Q, Options: "i"}}, {"description": primitive.Regex{Pattern: itemSM.Q, Options: "i"}}}})
	}
	if itemSM.PublishedBefore != nil && itemSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": itemSM.PublishedBefore, "$lte": itemSM.PublishedAfter}})
	} else if itemSM.PublishedAfter != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$lte": itemSM.PublishedAfter}})
	} else if itemSM.PublishedBefore != nil {
		query = append(query, bson.E{"publishedAt", bson.M{"$gt": itemSM.PublishedBefore}})
	}
	if itemSM.ChannelId != "" {
		query = append(query, bson.E{"channelId", itemSM.ChannelId})
	}
	if itemSM.ChannelType != "" {
		query = append(query, bson.E{"channelType", itemSM.ChannelType})
	}
	if itemSM.RelevanceLanguage != "" {
		query = append(query, bson.E{"relevanceLanguage", itemSM.RelevanceLanguage})
	}
	if itemSM.RegionCode != "" {
		query = append(query, bson.E{"blockedRegions", bson.M{"$ne": itemSM.RegionCode}})
	}
	return query
}

func sel(q ...string) (r bson.M) {
	r = make(bson.M, len(q))
	for _, s := range q {
		r[s] = 1
	}
	return
}

func getLimit(max int) int {
	if max == 0 {
		return 12
	} else {
		return max
	}
}

func getSkip(nextPageToken string) (*int, error) {
	a := 0
	if len(nextPageToken) > 0 {
		arr := strings.Split(nextPageToken, "|")
		if len(arr) < 2 {
			return nil, nil
		}
		if len(arr[0]) <= 0 {
			return &a, nil
		}
		i, err := strconv.Atoi(arr[0])
		if err != nil {
			return nil, err
		}
		return &i, nil
	}
	return &a, nil
}

func getNextPageToken(id string, len int, limit int, skip int) string {
	if len < limit {
		return ""
	} else {
		if len < 0 {
			return ""
		} else {
			return fmt.Sprintf(`%d|%s`, skip+limit, id)
		}
	}
}

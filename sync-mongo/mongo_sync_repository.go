package mongo

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/core-go/video"
)

type Id struct {
	Id string `json:"id,omitempty" `
}

type MongoVideoRepository struct {
	ChannelCollection       *mongo.Collection
	ChannelSyncCollection   *mongo.Collection
	PlaylistCollection      *mongo.Collection
	PlaylistVideoCollection *mongo.Collection
	VideoCollection         *mongo.Collection
	CategoryCollection      *mongo.Collection
}

func NewMongoVideoRepository(db *mongo.Database, channelCollectionName string, channelSyncCollectionName string, playlistCollectionName string, playlistVideoCollectionName string, videoCollectionName string, categoryCollection string) *MongoVideoRepository {
	return &MongoVideoRepository{
		ChannelCollection:       db.Collection(channelCollectionName),
		ChannelSyncCollection:   db.Collection(channelSyncCollectionName),
		PlaylistCollection:      db.Collection(playlistCollectionName),
		PlaylistVideoCollection: db.Collection(playlistVideoCollectionName),
		VideoCollection:         db.Collection(videoCollectionName),
		CategoryCollection:      db.Collection(categoryCollection),
	}
}

func (m *MongoVideoRepository) GetChannelSync(ctx context.Context, channelId string) (*ChannelSync, error) {
	query := bson.M{"_id": channelId}
	result := m.ChannelSyncCollection.FindOne(ctx, query)
	if result.Err() != nil {
		if strings.Compare(fmt.Sprint(result.Err()), "mongo: no documents in result") == 0 {
			return nil, nil
		} else {
			return nil, result.Err()
		}
	}
	channelSync := ChannelSync{}
	err := result.Decode(&channelSync)
	if err != nil {
		return nil, err
	}
	return &channelSync, nil
}

func (m *MongoVideoRepository) SaveChannel(ctx context.Context, channel Channel) (int64, error) {
	_, er1 := m.ChannelCollection.InsertOne(ctx, channel)
	if er1 != nil {
		errMsg := er1.Error()
		if strings.Contains(errMsg, "duplicate key error collection:") {
			query := bson.M{"_id": channel.Id}
			updateQuery := bson.M{
				"$set": channel,
			}
			result, err := m.ChannelCollection.UpdateOne(ctx, query, updateQuery)
			if result.ModifiedCount > 0 {
				return result.ModifiedCount, err
			} else if result.UpsertedCount > 0 {
				return result.UpsertedCount, err
			} else {
				return result.MatchedCount, err
			}
		} else {
			return 0, er1
		}
	}
	return 1, nil
}

func (m *MongoVideoRepository) GetVideoIds(ctx context.Context, ids []string) ([]string, error) {
	query := bson.M{"_id": bson.M{"$in": ids}}
	optionsFind := options.Find()
	optionsFind.SetProjection(bson.M{"_id": 1})
	result, er0 := m.VideoCollection.Find(ctx, query, optionsFind)
	if er0 != nil {
		return nil, er0
	}
	defer result.Close(ctx)
	var res []string
	for result.Next(ctx) {
		var id string
		if err := result.Decode(&id); err != nil {
			if strings.Contains(err.Error(), "cannot decode invalid into") {
				break
			} else {
				return nil, err
			}
		}
		res = append(res, id)
	}
	return res, nil
}

func (m *MongoVideoRepository) SaveVideos(ctx context.Context, videos []Video) (int, error) {
	arr := make([]interface{}, 0)
	values := reflect.Indirect(reflect.ValueOf(videos))
	length := values.Len()
	switch reflect.TypeOf(videos).Kind() {
	case reflect.Slice:
		for i := 0; i < length; i++ {
			arr = append(arr, values.Index(i).Interface())
		}
	}
	result, err := m.VideoCollection.InsertMany(ctx, arr)
	if err != nil {
		if err != nil {
			errMsg := err.Error()
			if strings.Contains(errMsg, "duplicate key error collection:") {
				res, er0 := SaveMany(ctx, m.VideoCollection, arr, "Id")
				if er0 != nil {
					return 0, er0
				}
				return int(res.UpsertedCount), er0
			}
		} else {
			return 0, err
		}
	}
	res := len(result.InsertedIDs)
	return res, nil
}

func (m *MongoVideoRepository) SavePlaylists(ctx context.Context, playlist []Playlist) (int, error) {
	arr := make([]interface{}, 0)
	values := reflect.Indirect(reflect.ValueOf(playlist))
	length := values.Len()
	switch reflect.TypeOf(playlist).Kind() {
	case reflect.Slice:
		for i := 0; i < length; i++ {
			arr = append(arr, values.Index(i).Interface())
		}
	}
	result, err := m.PlaylistCollection.InsertMany(ctx, arr)
	if err != nil {
		if err != nil {
			errMsg := err.Error()
			if strings.Contains(errMsg, "duplicate key error collection:") {
				res, er0 := SaveMany(ctx, m.PlaylistCollection, playlist, "Id")
				if er0 != nil {
					return 0, er0
				}
				return int(res.UpsertedCount), er0
			}
		} else {
			return 0, err
		}
	}
	res := len(result.InsertedIDs)
	return res, nil
}

func (m *MongoVideoRepository) SavePlaylistVideos(ctx context.Context, playlistId string, videos []string) (int, error) {
	playlistVideos := PlaylistVideoIdVideos{
		Id:     playlistId,
		Videos: videos,
	}
	_, er1 := m.PlaylistVideoCollection.InsertOne(ctx, playlistVideos)
	if er1 != nil {
		errMsg := er1.Error()
		if strings.Contains(errMsg, "duplicate key error collection:") {
			query := bson.M{"_id": playlistVideos.Id}
			updateQuery := bson.M{
				"$set": playlistVideos,
			}
			result, err := m.PlaylistVideoCollection.UpdateOne(ctx, query, updateQuery)
			if result.ModifiedCount > 0 {
				return int(result.ModifiedCount), err
			} else if result.UpsertedCount > 0 {
				return int(result.ModifiedCount), err
			} else {
				return int(result.ModifiedCount), err
			}
		} else {
			return 0, er1
		}
	}
	return 1, nil
}

func (m *MongoVideoRepository) SaveChannelSync(ctx context.Context, channel ChannelSync) (int, error) {
	_, er1 := m.ChannelSyncCollection.InsertOne(ctx, channel)
	if er1 != nil {
		errMsg := er1.Error()
		if strings.Contains(errMsg, "duplicate key error collection:") {
			query := bson.M{"_id": channel.Id}
			updateQuery := bson.M{
				"$set": channel,
			}
			result, err := m.ChannelSyncCollection.UpdateOne(ctx, query, updateQuery)
			if err != nil {
				return 0, err
			}
			if result.ModifiedCount > 0 {
				return int(result.ModifiedCount), err
			} else if result.UpsertedCount > 0 {
				return int(result.ModifiedCount), err
			} else {
				return int(result.ModifiedCount), err
			}
		} else {
			return 0, er1
		}
	}
	return 1, nil
}

func (m *MongoVideoRepository) SavePlaylist(ctx context.Context, playlist Playlist) (int, error) {
	_, er1 := m.PlaylistCollection.InsertOne(ctx, playlist)
	if er1 != nil {
		errMsg := er1.Error()
		if strings.Contains(errMsg, "duplicate key error collection:") {
			query := bson.M{"_id": playlist.Id}
			updateQuery := bson.M{
				"$set": playlist,
			}
			result, err := m.ChannelSyncCollection.UpdateOne(ctx, query, updateQuery)
			if err != nil {
				return 0, err
			}
			if result.ModifiedCount > 0 {
				return int(result.ModifiedCount), err
			} else if result.UpsertedCount > 0 {
				return int(result.ModifiedCount), err
			} else {
				return int(result.ModifiedCount), err
			}
		} else {
			return 0, er1
		}
	}
	return 1, nil
}

func SaveMany(ctx context.Context, collection *mongo.Collection, model interface{}, idName string) (*mongo.BulkWriteResult, error) { //Patch
	models := make([]mongo.WriteModel, 0)
	switch reflect.TypeOf(model).Kind() {
	case reflect.Slice:
		values := reflect.ValueOf(model)
		n := values.Len()
		if n > 0 {
			if index := findIndex(values.Index(0).Interface(), idName); index != -1 {
				for i := 0; i < n; i++ {
					row := values.Index(i).Interface()
					id, er0 := getValue(row, index)
					if er0 != nil {
						return nil, er0
					}
					if id != nil || (reflect.TypeOf(id).String() == "string") || (reflect.TypeOf(id).String() == "string" && len(id.(string)) > 0) { // if exist
						updateModel := mongo.NewReplaceOneModel().SetUpsert(true).SetReplacement(row).SetFilter(bson.M{"_id": id})
						models = append(models, updateModel)
					} else {
						insertModel := mongo.NewInsertOneModel().SetDocument(row)
						models = append(models, insertModel)
					}
				}
			}
		}
	}
	rs, err := collection.BulkWrite(ctx, models)
	return rs, err
}

func getValue(model interface{}, index int) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	return vo.Field(index).Interface(), nil
}

func findIndex(model interface{}, fieldName string) int {
	modelType := reflect.Indirect(reflect.ValueOf(model))
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		if modelType.Type().Field(i).Name == fieldName {
			return i
		}
	}
	return -1
}

package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/core-go/video"
	"github.com/core-go/video/category"
	"github.com/gocql/gocql"
)

type CassandraVideoService struct {
	session                  *gocql.Session
	tubeCategory             category.CategorySyncClient
	channelFieldsIndex       map[string]int
	playlistFieldsIndex      map[string]int
	videoFieldsIndex         map[string]int
	playlistVideoFieldsIndex map[string]int
	categoryFieldsIndex      map[string]int
}

func NewCassandraVideoService(cass *gocql.ClusterConfig, tubeCategory category.CategorySyncClient) (*CassandraVideoService,error) {
	session, er0 := cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	var channel video.Channel
	channelReflect := reflect.TypeOf(channel)
	channelFieldsIndex,err := GetColumnIndexes(channelReflect)
	if err != nil {
		return nil, err
	}
	var playlist video.Playlist
	playlistReflect := reflect.TypeOf(playlist)
	playlistFieldsIndex,err := GetColumnIndexes(playlistReflect)
	if err != nil {
		return nil, err
	}
	var videoV video.Video
	videoReflect := reflect.TypeOf(videoV)
	videoFieldsIndex,err := GetColumnIndexes(videoReflect)
	if err != nil {
		return nil, err
	}
	var playlistVideo video.PlaylistVideoIdVideos
	playlistVideoReflect := reflect.TypeOf(playlistVideo)
	playlistVideoFieldsIndex,err := GetColumnIndexes(playlistVideoReflect)
	if err != nil {
		return nil, err
	}
	var category video.Categories
	categoryReflect := reflect.TypeOf(category)
	categoryFieldsIndex,err := GetColumnIndexes(categoryReflect)
	if err != nil {
		return nil, err
	}
	return &CassandraVideoService{
		session:                  session,
		tubeCategory:             tubeCategory,
		channelFieldsIndex:       channelFieldsIndex,
		playlistFieldsIndex:      playlistFieldsIndex,
		videoFieldsIndex:         videoFieldsIndex,
		playlistVideoFieldsIndex: playlistVideoFieldsIndex,
		categoryFieldsIndex:      categoryFieldsIndex,
	},nil
}

func (c *CassandraVideoService) GetChannel(ctx context.Context, channelId string, fields []string) (*video.Channel, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from channel where id = ?`, strings.Join(fields, ","))
	var channel []video.Channel
	err := Query(c.session, c.channelFieldsIndex, &channel, query, channelId)
	if err != nil {
		return nil, err
	}
	if len(channel) <= 0{
		return nil, nil
	}
	return &channel[0], nil
}

func (c *CassandraVideoService) GetChannels(ctx context.Context, ids []string, fields []string) (*[]video.Channel, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from channel where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var channel []video.Channel
	err := Query(c.session, c.channelFieldsIndex,&channel, query, cc...)
	if err != nil {
		return nil, err
	}
	return &channel, nil
}

func (c *CassandraVideoService) GetPlaylist(ctx context.Context, id string, fields []string) (*video.Playlist, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from playlist where id = ?`, strings.Join(fields, ","))
	var playlist []video.Playlist
	err := Query(c.session, c.playlistFieldsIndex,&playlist, query, id)
	if err != nil {
		return nil, err
	}
	if len(playlist) <= 0 {
		return nil, nil
	}
	return &playlist[0], nil
}

func (c *CassandraVideoService) GetPlaylists(ctx context.Context, ids []string, fields []string) (*[]video.Playlist, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from playlist where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var playlist []video.Playlist
	err := Query(c.session, c.playlistFieldsIndex, &playlist, query, cc...)
	if err != nil {
		return nil, err
	}
	return &playlist, nil
}

func (c *CassandraVideoService) GetVideo(ctx context.Context, id string, fields []string) (*video.Video, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from video where id = ?`, strings.Join(fields, ","))
	var video []video.Video
	err := Query(c.session, c.videoFieldsIndex, &video, query, id)
	if err != nil {
		return nil, err
	}
	if len(video) <= 0 {
		return nil,nil
	}
	return &video[0], nil
}

func (c *CassandraVideoService) GetVideos(ctx context.Context, ids []string, fields []string) (*[]video.Video, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from video where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var video []video.Video
	err := Query(c.session, c.videoFieldsIndex, &video, query, cc...)
	if err != nil {
		return nil, err
	}
	return &video, nil
}

func (c *CassandraVideoService) GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	sort := map[string]interface{}{"field": `publishedat`, "reverse": true}
	must := map[string]interface{}{"type": "match", "field": "channelid", "value": fmt.Sprintf(`%s`, channelId)}
	a := map[string]interface{}{
		"filter": map[string]interface{}{
			"must": must,
		},
		"sort": sort,
	}
	queryObj, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	sql := fmt.Sprintf(`select %s from playlist where expr(playlist_index, '%s')`, strings.Join(fields, ","), queryObj)
	var listResultPlaylist video.ListResultPlaylist
	var value []interface{}
	listResultPlaylist.NextPageToken,err = QueryWithPage(c.session, c.playlistFieldsIndex, &listResultPlaylist.List, sql, value, max, nextPageToken)
	if err != nil {
		return nil, err
	}
	listResultPlaylist.Limit = max
	return &listResultPlaylist, nil
}

func (c *CassandraVideoService) GetChannelVideos(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	sort := map[string]interface{}{"field": `publishedat`, "reverse": true}
	must := map[string]interface{}{"type": "match", "field": "channelid", "value": fmt.Sprintf(`%s`, channelId)}
	a := map[string]interface{}{
		"filter": map[string]interface{}{
			"must": must,
		},
		"sort": sort,
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	queryObj, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`select %s from video where expr(video_index, '%s')`, strings.Join(fields, ","), queryObj)
	var resList video.ListResultVideos
	var value []interface{}
	resList.NextPageToken,err = QueryWithPage(c.session, c.videoFieldsIndex, &resList.List, sql, value, max, nextPageToken)
	if err != nil {
		return nil, err
	}
	resList.Limit = max
	return &resList, nil
}

func (c *CassandraVideoService) GetPlaylistVideos(ctx context.Context, playlistId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	var sql = `select * from playlistVideo where id = ?`
	var playlistVideo []video.PlaylistVideoIdVideos
	er1 := Query(c.session, c.playlistVideoFieldsIndex,&playlistVideo, sql, playlistId)
	if er1 != nil {
		return nil, er1
	}
	if len(playlistVideo) == 0 {
		return nil, nil
	}
	question := make([]string, len(playlistVideo[0].Videos))
	cc := make([]interface{}, len(playlistVideo[0].Videos))
	for i, v := range playlistVideo[0].Videos {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	queryV := fmt.Sprintf(`select %s from video where id in (%s) limit %d`, strings.Join(fields, ","), strings.Join(question, ","), max)
	var res video.ListResultVideos
	res.NextPageToken,er1 = QueryWithPage(c.session, c.videoFieldsIndex, &res.List, queryV, cc, max, nextPageToken)
	if er1 != nil {
		return nil, er1
	}
	res.Limit = max
	return &res, nil
}

func (c *CassandraVideoService) GetCategories(ctx context.Context, regionCode string) (*video.Categories, error) {
	sql := `select * from category where id = ?`
	var categories []video.Categories
	err := Query(c.session, c.categoryFieldsIndex,&categories, sql, regionCode)
	if err != nil {
		return nil, err
	}
	if len(categories) == 0 {
		res, er1 := c.tubeCategory.GetCagetories(regionCode)
		if er1 != nil {
			return nil, er1
		}
		query := "insert into category (id,data) values (?, ?)"
		result := video.Categories{
			Id:   regionCode,
			Data: *res,
		}
		_, err := Exec(c.session, query, result.Id, result.Data)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}
	return &categories[0], nil
}

func (c *CassandraVideoService) SearchChannel(ctx context.Context, channelSM video.ChannelSM, max int, nextPageToken string, fields []string) (*video.ListResultChannel, error) {
	sql, err := buildChannelSearch(channelSM, fields)
	if err != nil {
		return nil, err
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	var res video.ListResultChannel
	var value []interface{}
	res.NextPageToken, err = QueryWithPage(c.session, c.channelFieldsIndex, &res.List, sql, value, max, nextPageToken)
	res.Limit = max
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *CassandraVideoService) SearchPlaylists(ctx context.Context, playlistSM video.PlaylistSM, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	sql, err := buildPlaylistSearch(playlistSM, fields)
	if err != nil {
		return nil, err
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	var res video.ListResultPlaylist
	var value []interface{}
	res.NextPageToken, err = QueryWithPage(c.session, c.playlistFieldsIndex, &res.List, sql, value, max, nextPageToken)
	res.Limit = max
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *CassandraVideoService) SearchVideos(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	sql, err := buildVideosSearch(itemSM, fields)
	if err != nil {
		return nil, err
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	var res video.ListResultVideos
	var value []interface{}
	res.NextPageToken, err = QueryWithPage(c.session, c.videoFieldsIndex, &res.List, sql, value, max, nextPageToken)
	if err != nil {
		return nil, err
	}
	res.Limit = max
	return &res, nil
}

func (c *CassandraVideoService) Search(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	sql, err := buildVideosSearch(itemSM, fields)
	if err != nil {
		return nil, err
	}
	var res video.ListResultVideos
	var value []interface{}
	res.NextPageToken, err = QueryWithPage(c.session, c.videoFieldsIndex, &res.List, sql, value, max, nextPageToken)
	if err != nil {
		return nil, err
	}
	res.Limit = max
	return &res, nil
}

func (c *CassandraVideoService) GetRelatedVideos(ctx context.Context, videoId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	var a []string
	resVd, err := c.GetVideo(ctx, videoId, a)
	if err != nil {
		return nil, err
	}
	if resVd == nil {
		return nil, errors.New("video don't exist")
	} else {
		var should []interface{}
		for _, v := range resVd.Tags {
			should = append(should, map[string]interface{}{"type": "contains", "field": "tags", "values": v})
		}
		not := map[string]interface{}{"type": "match", "field": "id", "value": videoId}
		sort := map[string]interface{}{"field": "publishedat", "reverse": true}
		fields = checkFields("publishedAt", fields)
		a := map[string]interface{}{
			"filter": map[string]interface{}{
				"should": should,
				"not":    not,
			},
			"sort": sort,
		}
		queryObj, err := json.Marshal(a)
		if err != nil {
			return nil, err
		}
		if len(fields) <= 0 {
			fields = append(fields, "*")
		}
		sql := fmt.Sprintf(`select %s from video where expr(video_index,'%s')`, strings.Join(fields, ","), queryObj)
		var res video.ListResultVideos
		var value []interface{}
		res.NextPageToken, err = QueryWithPage(c.session, c.videoFieldsIndex, &res.List, sql, value, max, nextPageToken)
		res.Limit = max
		return &res, nil
	}
}

func (c *CassandraVideoService) GetPopularVideos(ctx context.Context, regionCode string, categoryId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	var query []interface{}
	var not []interface{}
	if len(regionCode) > 0 {
		not = append(not, map[string]interface{}{"type": "contains", "field": "blockedregions", "values": regionCode})
	}
	if len(categoryId) > 0 {
		query = append(query, map[string]interface{}{"type": "match", "field": "categoryid", "value": categoryId})
		fields = checkFields("categoryId", fields)
	}
	sort := map[string]interface{}{"field": "publishedat", "reverse": true}
	fields = checkFields("publishedAt", fields)
	a := map[string]interface{}{
		"filter": map[string]interface{}{
			"not": not,
		},
		"query": query,
		"sort":  sort,
	}
	if len(not) == 0 {
		delete(a, "filter")
	}
	if len(query) == 0 {
		delete(a, "query")
	}
	queryObj, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	sql := fmt.Sprintf(`select %s from video where expr(video_index,'%s')`, strings.Join(fields, ","), queryObj)
	var res video.ListResultVideos
	var value []interface{}
	res.NextPageToken, err = QueryWithPage(c.session, c.videoFieldsIndex, &res.List, sql, value, max, nextPageToken)
	res.Limit = max
	return &res, nil
}

func buildChannelSearch(s video.ChannelSM, fields []string) (string, error) {
	var should []interface{}
	var must []interface{}
	var not []interface{}
	var sort []interface{}
	if len(s.Q) > 0 {
		should = append(should, map[string]interface{}{"type": "phrase", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "phrase", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
	}
	if s.PublishedBefore != nil && s.PublishedAfter != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1, "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedAfter != nil {
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedBefore != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1})
		fields = checkFields("publishedAt", fields)
	}
	if len(s.ChannelId) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "id", "value": s.ChannelId})
		fields = checkFields("id", fields)
	}
	if len(s.ChannelType) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "channeltype", "value": s.ChannelType})
		fields = checkFields("channelType", fields)
	}
	if len(s.TopicId) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "topicid", "value": s.TopicId})
		fields = checkFields("topicId", fields)
	}
	if len(s.RegionCode) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "country", "value": s.RegionCode})
		fields = checkFields("country", fields)
	}
	if len(s.RelevanceLanguage) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "relevancelanguage", "value": s.RelevanceLanguage})
		fields = checkFields("relevanceLanguage", fields)
	}
	if len(s.Sort) > 0 {
		sort = append(sort, map[string]interface{}{"field": strings.ToLower(s.Sort), "reverse": true})
		fields = checkFields(s.Sort, fields)
	}
	filter := map[string]interface{}{
		"should": should,
		"not":    not,
	}
	a := map[string]interface{}{
		"filter": filter,
		"query":  map[string]interface{}{"must": must},
		"sort":   sort,
	}
	if len(should) == 0 && len(not) == 0 {
		delete(a, "filter")
	} else {
		if len(should) == 0 {
			delete(filter, "should")
		}
		if len(not) == 0 {
			delete(filter, "not")
		}
	}
	if len(must) == 0 {
		delete(a, "query")
	}
	if len(sort) == 0 {
		delete(a, "sort")
	}
	queryObj, err := json.Marshal(a)
	if err != nil {
		return "", err
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	sql := fmt.Sprintf(`select %s from channel where expr(channel_index,'%s')`, strings.Join(fields, ","), queryObj)
	return sql, nil
}

func buildPlaylistSearch(s video.PlaylistSM, fields []string) (string, error) {
	var should []interface{}
	var must []interface{}
	var not []interface{}
	var sort []interface{}
	if len(s.Q) > 0 {
		should = append(should, map[string]interface{}{"type": "phrase", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "phrase", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
	}
	if s.PublishedBefore != nil && s.PublishedAfter != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1, "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedAfter != nil {
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedBefore != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1})
		fields = checkFields("publishedAt", fields)
	}
	if len(s.ChannelId) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "channelid", "value": s.ChannelId})
		fields = checkFields("channelId", fields)
	}
	if len(s.ChannelType) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "channeltype", "value": s.ChannelType})
		fields = checkFields("channelType", fields)
	}
	if len(s.RegionCode) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "country", "value": s.RegionCode})
		fields = checkFields("country", fields)
	}
	if len(s.RelevanceLanguage) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "relevancelanguage", "value": s.RelevanceLanguage})
		fields = checkFields("relevanceLanguage", fields)
	}
	if len(s.Sort) > 0 {
		sort = append(sort, map[string]interface{}{"field": strings.ToLower(s.Sort), "reverse": true})
	}
	filter := map[string]interface{}{
		"should": should,
		"not":    not,
	}
	a := map[string]interface{}{
		"filter": filter,
		"query":  map[string]interface{}{"must": must},
		"sort":   sort,
	}
	if len(should) == 0 && len(not) == 0 {
		delete(a, "filter")
	} else {
		if len(should) == 0 {
			delete(filter, "should")
		}
		if len(not) == 0 {
			delete(filter, "not")
		}
	}
	if len(must) == 0 {
		delete(a, "query")
	}
	if len(sort) == 0 {
		delete(a, "sort")
	}

	queryObj, err := json.Marshal(a)
	if err != nil {
		return "", err
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	sql := fmt.Sprintf(`select %s from playlist where expr(playlist_index,'%s')`, strings.Join(fields, ","), queryObj)
	return sql, nil
}

func buildVideosSearch(s video.ItemSM, fields []string) (string, error) {
	var should []interface{}
	var must []interface{}
	var not []interface{}
	var sort []interface{}
	if len(s.Duration) > 0 {
		switch s.Duration {
		case "short":
			must = append(must, map[string]interface{}{"type": "range", "field": "duration", "upper": "240"})
			break
		case "medium":
			must = append(must, map[string]interface{}{"type": "range", "field": "duration", "lower": "240", "upper": "1200"})
			break
		case "long":
			must = append(must, map[string]interface{}{"type": "range", "field": "duration", "lower": "1200"})
			break
		default:
			break
		}
		fields = checkFields("duration", fields)
	}
	if len(s.Q) > 0 {
		should = append(should, map[string]interface{}{"type": "phrase", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "title", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "title", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "phrase", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "prefix", "field": "description", "value": fmt.Sprintf(`%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`%s*`, s.Q)})
		should = append(should, map[string]interface{}{"type": "wildcard", "field": "description", "value": fmt.Sprintf(`*%s*`, s.Q)})
	}
	if s.PublishedBefore != nil && s.PublishedAfter != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1, "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedAfter != nil {
		t2 := s.PublishedAfter.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "upper": t2})
		fields = checkFields("publishedAt", fields)
	} else if s.PublishedBefore != nil {
		t1 := s.PublishedBefore.Format("2006-01-02 15:04:05")
		must = append(must, map[string]interface{}{"type": "range", "field": "publishedat", "lower": t1})
		fields = checkFields("publishedAt", fields)
	}
	if len(s.RegionCode) > 0 {
		not = append(not, map[string]interface{}{"type": "match", "field": "blockedregions", "value": s.RegionCode})
	}
	if len(s.Sort) > 0 {
		sort = append(sort, map[string]interface{}{"field": strings.ToLower(s.Sort), "reverse": true})
		fields = checkFields(s.Sort, fields)
	}
	filter := map[string]interface{}{
		"should": should,
		"not":    not,
	}
	a := map[string]interface{}{
		"filter": filter,
		"query":  map[string]interface{}{"must": must},
		"sort":   sort,
	}
	if len(should) == 0 && len(not) == 0 {
		delete(a, "filter")
	} else {
		if len(should) == 0 {
			delete(filter, "should")
		}
		if len(not) == 0 {
			delete(filter, "not")
		}
	}
	if len(must) == 0 {
		delete(a, "query")
	}
	if len(sort) == 0 {
		delete(a, "sort")
	}
	queryObj, err := json.Marshal(a)
	if err != nil {
		return "", err
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	sql := fmt.Sprintf(`select %s from video where expr(video_index,'%s')`, strings.Join(fields, ","), queryObj)
	return sql, nil
}

func checkFields(check string, fields []string) []string {
	if len(fields) == 0 {
		return fields
	}
	flag := false
	for _, v := range fields {
		if v == check {
			flag = true
			break
		}
	}
	if !flag {
		fields = append(fields, check)
	}
	return fields
}

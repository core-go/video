package cassandra

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	cassql "github.com/core-go/cassandra"
	"github.com/core-go/video"
	"github.com/core-go/video/category"
	"github.com/gocql/gocql"
	"log"
	"strings"
	"time"
)

type CassandraVideoService struct {
	cass         *gocql.ClusterConfig
	tubeCategory category.CategorySyncClient
}

func NewCassandraVideoService(cass *gocql.ClusterConfig, tubeCategory category.CategorySyncClient) *CassandraVideoService {
	return &CassandraVideoService{
		cass: cass,
		tubeCategory: tubeCategory,
	}
}

func (c *CassandraVideoService) GetChannel(ctx context.Context, channelId string, fields []string) (*video.Channel, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from channel where id = ?`, strings.Join(fields, ","))
	//q := session.Query(query, channelId)
	//if q.Exec() != nil {
	//	return nil, q.Exec()
	//}
	//q.Iter()
	//res := channelConvert(q.Iter())
	//if len(res) > 0 && len(res[0].ChannelList) > 0 {
	//	channels,err := c.GetChannels(ctx, res[0].ChannelList, []string{})
	//	if err != nil {
	//		return nil, err
	//	}
	//	res[0].Channels = *channels
	//}
	//if len(res) == 0 {
	//	return nil, nil
	//}
	//return &res[0], nil
	var channel []video.Channel
	err := cassql.Query(session, &channel, query, channelId)
	if err != nil {
		return nil, err
	}
	return &channel[0], nil
}

func (c *CassandraVideoService) GetChannels(ctx context.Context, ids []string, fields []string) (*[]video.Channel, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from channel where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	//q := session.Query(query, cc...)
	//if q.Exec() != nil {
	//	return nil, q.Exec()
	//}
	//res := channelConvert(q.Iter())
	//return &res, nil
	var channel []video.Channel
	err := cassql.Query(session, &channel, query, cc...)
	if err != nil {
		return nil, err
	}
	return &channel, nil
}

func (c *CassandraVideoService) GetPlaylist(ctx context.Context, id string, fields []string) (*video.Playlist, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from playlist where id = ?`, strings.Join(fields, ","))
	//rows := session.Query(query, id)
	//if rows.Exec() != nil {
	//	return nil, rows.Exec()
	//}
	//res := playlistConvert(rows.Iter())
	//if len(res) == 0 {
	//	return nil, nil
	//}
	var playlist []video.Playlist
	err := cassql.Query(session, &playlist, query, id)
	if err != nil {
		return nil, err
	}
	return &playlist[0], nil
}

func (c *CassandraVideoService) GetPlaylists(ctx context.Context, ids []string, fields []string) (*[]video.Playlist, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from playlist where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	//rows := session.Query(query, cc...)
	//if rows.Exec() != nil {
	//	return nil, rows.Exec()
	//}
	//result := playlistConvert(rows.Iter())
	var playlist []video.Playlist
	err := cassql.Query(session, &playlist, query, cc...)
	if err != nil {
		return nil, err
	}
	return &playlist, nil
}

func (c *CassandraVideoService) GetVideo(ctx context.Context, id string, fields []string) (*video.Video, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from video where id = ?`, strings.Join(fields, ","))
	//rows := session.Query(query, id)
	//if rows.Exec() != nil {
	//	return nil, rows.Exec()
	//}
	//res := videoConvert(rows.Iter())
	//if len(res) == 0 {
	//	return nil, nil
	//}
	//return &res[0], nil
	var video []video.Video
	err := cassql.Query(session, &video, query, id)
	if err != nil {
		return nil, err
	}
	return &video[0], nil
}

func (c *CassandraVideoService) GetVideos(ctx context.Context, ids []string, fields []string) (*[]video.Video, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`Select %s from video where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	//rows := session.Query(query, cc...)
	//if rows.Exec() != nil {
	//	return nil, rows.Exec()
	//}
	//res := videoConvert(rows.Iter())
	//return &res, nil
	var video []video.Video
	err := cassql.Query(session, &video, query, cc...)
	if err != nil {
		return nil, err
	}
	return &video, nil
}

func (c *CassandraVideoService) GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	//sort := map[string]interface{}{"field": `publishedat`, "reverse": true}
	//must := map[string]interface{}{"type": "match", "field": "channelid", "value": fmt.Sprintf(`%s`, channelId)}
	//a := map[string]interface{}{
	//	"filter": map[string]interface{}{
	//		"must": must,
	//	},
	//	"sort": sort,
	//}
	//queryObj, err := json.Marshal(a)
	//if err != nil {
	//	return nil, err
	//}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	//sql := fmt.Sprintf(`select %s from playlist where expr(playlist_index, '%s')`, strings.Join(fields, ","), queryObj)
	sql := fmt.Sprintf(`select %s from playlist where channelid = ? limit %d ALLOW FILTERING`, strings.Join(fields, ","), max)
	//var query *gocql.Query
	//next, er1 := hex.DecodeString(nextPageToken)
	//if er1 != nil {
	//	return nil, er1
	//}
	//query = session.Query(sql).PageState(next).PageSize(max)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	//iter := query.Iter()
	//var res video.ListResultPlaylist
	//res.NextPageToken = hex.EncodeToString(iter.PageState())
	//res.List = playlistConvert(iter)
	//return &res, nil
	var listResultPlaylist video.ListResultPlaylist
	err := cassql.Query(session, &listResultPlaylist.List, sql, channelId)
	if err != nil {
		return nil, err
	}
	listResultPlaylist.Total = len(listResultPlaylist.List)
	if listResultPlaylist.Total > 0 {
		listResultPlaylist.Limit = max
	}
	return &listResultPlaylist, nil
}

func (c *CassandraVideoService) GetChannelVideos(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	//sort := map[string]interface{}{"field": `publishedat`, "reverse": true}
	//must := map[string]interface{}{"type": "match", "field": "channelid", "value": fmt.Sprintf(`%s`, channelId)}
	//a := map[string]interface{}{
	//	"filter": map[string]interface{}{
	//		"must": must,
	//	},
	//	"sort": sort,
	//}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	//queryObj, err := json.Marshal(a)
	//if err != nil {
	//	return nil, err
	//}
	//sql := fmt.Sprintf(`select %s from video where expr(video_index, '%s')`, strings.Join(fields, ","), queryObj)
	//var query *gocql.Query
	//next, err := hex.DecodeString(nextPageToken)
	//if err != nil {
	//	return nil, err
	//}
	//query = session.Query(sql).PageState(next).PageSize(max)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	//iter := query.Iter()

	sql := fmt.Sprintf(`select %s from video where channelid = ? limit %d ALLOW FILTERING`, strings.Join(fields, ","), max)
	var resList video.ListResultVideos
	err := cassql.Query(session, &resList.List, sql, channelId)
	if err != nil {
		return nil, err
	}
	resList.Total = len(resList.List)
	if resList.Total > 0 {
		resList.Limit = max
	}
	return &resList, nil
}

func (c *CassandraVideoService) GetPlaylistVideos(ctx context.Context, playlistId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	var sql = `select * from playlistVideo where id = ?`
	//query := session.Query(sql, playlistId)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	var ids []video.PlaylistVideoIdVideos
	er1 := cassql.Query(session, &ids, sql, playlistId)
	if er1 != nil {
		return nil, er1
	}
	//query.Iter().Scan(&ids)

	question := make([]string, len(ids[0].Videos))
	cc := make([]interface{}, len(ids[0].Videos))
	for i, v := range ids[0].Videos {
		question[i] = "?"
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	//next, err := hex.DecodeString(nextPageToken)
	//if err != nil {
	//	return nil, err
	//}
	queryV := fmt.Sprintf(`Select %s from video where id in (%s) limit %d`, strings.Join(fields, ","), strings.Join(question, ","), max)
	var res video.ListResultVideos
	er2 := cassql.Query(session, &res.List, queryV, cc...)
	if er2 != nil {
		return nil, er2
	}
	//rows := session.Query(queryV, cc...).PageState(next).PageSize(max)
	//if rows.Exec() != nil {
	//	return nil, rows.Exec()
	//}
	res.Total = len(res.List)
	if res.Total > 0 {
		res.Limit = max
	}
	return &res, nil
}

func (c *CassandraVideoService) GetCategories(ctx context.Context, regionCode string) (*video.Categories, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	sql := `select * from category where id = ?`
	//query := session.Query(sql, regionCode)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	//var category video.Categories
	//query.Iter().Scan(&category.Id, &category.Data)
	var categories []video.Categories
	err := cassql.Query(session, &categories, sql, regionCode)
	if err != nil {
		return nil, err
	}
	//if category.Data == nil {
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
		err := session.Query(query, result.Id, result.Data).Exec()
		if err != nil {
			return nil, err
		}
		return &result, nil
	}
	return &categories[0], nil
}

func (c *CassandraVideoService) SearchChannel(ctx context.Context, channelSM video.ChannelSM, max int, nextPageToken string, fields []string) (*video.ListResultChannel, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	sql, er1 := buildChannelSearch(channelSM, fields)
	if er1 != nil {
		return nil, er1
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	//next, err := hex.DecodeString(nextPageToken)
	//if err != nil {
	//	return nil, err
	//}
	//query := session.Query(sql).PageState(next).PageSize(max)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	var resChannel []video.Channel
	var res video.ListResultChannel
	err := cassql.Query(session, &resChannel, sql)
	if err != nil {
		return nil, err
	}
	res.List = resChannel
	res.Total = len(resChannel)
	if res.Total > 0 {
		res.Limit = max
	}
	//res.List = channelConvert(query.Iter())
	//res.NextPageToken = hex.EncodeToString(query.Iter().PageState())
	return &res, nil
}

func (c *CassandraVideoService) SearchPlaylists(ctx context.Context, playlistSM video.PlaylistSM, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	sql, er1 := buildPlaylistSearch(playlistSM, fields)
	if er1 != nil {
		return nil, er1
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	//next, err := hex.DecodeString(nextPageToken)
	//if err != nil {
	//	return nil, err
	//}
	//query := session.Query(sql).PageState(next).PageSize(max)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	var res video.ListResultPlaylist
	var resPlaylist []video.Playlist
	err := cassql.Query(session, &resPlaylist, sql)
	if err != nil {
		return nil, err
	}
	res.List = resPlaylist
	res.Total = len(resPlaylist)
	if res.Total > 0 {
		res.Limit = max
	}
	//res.List = playlistConvert(query.Iter())
	//res.NextPageToken = hex.EncodeToString(query.Iter().PageState())
	return &res, nil
}

func (c *CassandraVideoService) SearchVideos(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	sql, er1 := buildVideosSearch(itemSM, fields)
	if er1 != nil {
		return nil, er1
	}
	sql = sql + fmt.Sprintf(` limit %d`, max)
	//next, err := hex.DecodeString(nextPageToken)
	//if err != nil {
	//	return nil, err
	//}
	//query := session.Query(sql).PageState(next).PageSize(max)
	//if query.Exec() != nil {
	//	return nil, query.Exec()
	//}
	var res video.ListResultVideos
	var resVideo []video.Video
	err := cassql.Query(session, &resVideo, sql)
	if err != nil {
		return nil, err
	}
	res.List = resVideo
	res.Total = len(resVideo)
	if res.Total > 0 {
		res.Limit = max
	}
	//res.List = videoConvert(query.Iter())
	//res.NextPageToken = hex.EncodeToString(query.Iter().PageState())
	return &res, nil
}

func (c *CassandraVideoService) Search(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	sql, er1 := buildVideosSearch(itemSM, fields)
	if er1 != nil {
		return nil, er1
	}
	next, err := hex.DecodeString(nextPageToken)
	if err != nil {
		return nil, err
	}
	query := session.Query(sql).PageState(next).PageSize(max)
	if query.Exec() != nil {
		return nil, query.Exec()
	}
	var res video.ListResultVideos
	res.List = videoConvert(query.Iter())
	res.NextPageToken = hex.EncodeToString(query.Iter().PageState())
	return &res, nil
}

func (c *CassandraVideoService) GetRelatedVideos(ctx context.Context, videoId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	var a []string
	resVd, er1 := c.GetVideo(ctx, videoId, a)
	if er1 != nil {
		return nil, er1
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
		next, err := hex.DecodeString(nextPageToken)
		if err != nil {
			return nil, err
		}
		query := session.Query(sql).PageState(next).PageSize(max)
		var res video.ListResultVideos
		res.List = videoConvert(query.Iter())
		res.NextPageToken = hex.EncodeToString(query.Iter().PageState())
		return &res, nil
	}
}

func (c *CassandraVideoService) GetPopularVideos(ctx context.Context, regionCode string, categoryId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	session, er0 := c.cass.CreateSession()
	if er0 != nil {
		return nil, er0
	}
	query := []interface{}{}
	not := []interface{}{}
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
	next, err := hex.DecodeString(nextPageToken)
	if err != nil {
		return nil, err
	}
	q := session.Query(sql).PageState(next).PageSize(max)
	var res video.ListResultVideos
	res.List = videoConvert(q.Iter())
	res.NextPageToken = hex.EncodeToString(q.Iter().PageState())
	return &res, nil
}

func buildChannelSearch(s video.ChannelSM, fields []string) (string, error) {
	should := []interface{}{}
	must := []interface{}{}
	not := []interface{}{}
	sort := []interface{}{}
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
	log.Println(sql)
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
	if len(s.ChannelId) > 0 {
		must = append(must, map[string]interface{}{"type": "match", "field": "channelid", "value": s.ChannelId})
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
	fmt.Println("buildVideoSearch: ", sql)
	return sql, nil
}

func channelConvert(iter *gocql.Iter) []video.Channel {
	res := make([]video.Channel, iter.NumRows())
	//   &channel.PublishedAt, &channel.Thumbnail, &channel.Title, &channel.Uploads
	for i, _ := range res {
		row1 := make(map[string]interface{})
		if !iter.MapScan(row1) {
			break
		} else {
			if id, ok := row1["id"]; ok {
				res[i].Id = id.(string)
			}
			if count, ok := row1["count"]; ok {
				res[i].Count = count.(int)
			}
			if country, ok := row1["country"]; ok {
				res[i].Country = country.(string)
			}
			if customUrl, ok := row1["customurl"]; ok {
				res[i].CustomUrl = customUrl.(string)
			}
			if description, ok := row1["description"]; ok {
				res[i].Description = description.(string)
			}
			if favorites, ok := row1["favorites"]; ok {
				res[i].Favorites = favorites.(string)
			}
			if highThumbnail, ok := row1["highthumbnail"]; ok {
				h := highThumbnail.(string)
				res[i].HighThumbnail = &h
			}
			if itemCount, ok := row1["itemcount"]; ok {
				res[i].ItemCount = itemCount.(int)
			}
			if lastUpload, ok := row1["lastupload"]; ok {
				a := lastUpload.(time.Time)
				res[i].LastUpload = &a
			}
			if likes, ok := row1["likes"]; ok {
				res[i].Likes = likes.(string)
			}
			if localizedDescription, ok := row1["localizeddescription"]; ok {
				res[i].LocalizedDescription = localizedDescription.(string)
			}
			if playlistCount, ok := row1["playlistcount"]; ok {
				p := playlistCount.(int)
				res[i].PlaylistCount = &p
			}
			if PlaylistItemCount, ok := row1["playlistitemcount"]; ok {
				p := PlaylistItemCount.(int)
				res[i].PlaylistItemCount = &p
			}
			if PlaylistVideoCount, ok := row1["playlistvideocount"]; ok {
				p := PlaylistVideoCount.(int)
				res[i].PlaylistVideoCount = &p
			}

			if PlaylistVideoItemCount, ok := row1["playlistvideoitemcount"]; ok {
				p := PlaylistVideoItemCount.(int)
				res[i].PlaylistVideoItemCount = &p
			}
			if PublishedAt, ok := row1["publishedat"]; ok {
				a := PublishedAt.(time.Time)
				res[i].PublishedAt = &a
			}
			if Thumbnail, ok := row1["thumbnail"]; ok {
				t := Thumbnail.(string)
				res[i].Thumbnail = &t
			}
			if Title, ok := row1["title"]; ok {
				res[i].Title = Title.(string)
			}
			if Uploads, ok := row1["uploads"]; ok {
				res[i].Uploads = Uploads.(string)
			}
			if Channels, ok := row1["channels"]; ok {
				res[i].ChannelList = Channels.([]string)
			}
		}

	}
	return res
}

func playlistConvert(iter *gocql.Iter) []video.Playlist {
	res := make([]video.Playlist, iter.NumRows())
	//  &playlist.Thumbnail, &playlist.Title
	for i, _ := range res {
		row1 := make(map[string]interface{})
		if !iter.MapScan(row1) {
			break
		} else {
			if Id, ok := row1["id"]; ok {
				res[i].Id = Id.(string)
			}
			if ChannelId, ok := row1["channelid"]; ok {
				res[i].ChannelId = ChannelId.(string)
			}
			if ChannelTitle, ok := row1["channeltitle"]; ok {
				res[i].ChannelTitle = ChannelTitle.(string)
			}
			if Count, ok := row1["count"]; ok {
				c := Count.(int)
				res[i].Count = &c
			}
			if Description, ok := row1["description"]; ok {
				res[i].Description = Description.(string)
			}
			if HighThumbnail, ok := row1["highthumbnail"]; ok {
				h := HighThumbnail.(string)
				res[i].HighThumbnail = &h
			}
			if ItemCount, ok := row1["itemcount"]; ok {
				it := ItemCount.(int)
				res[i].ItemCount = &it
			}
			if LocalizedDescription, ok := row1["localizeddescription"]; ok {
				res[i].LocalizedDescription = LocalizedDescription.(string)
			}
			if LocalizedTitle, ok := row1["localizedtitle"]; ok {
				res[i].LocalizedTitle = LocalizedTitle.(string)
			}
			if MaxresThumbnail, ok := row1["maxresthumbnail"]; ok {
				m := MaxresThumbnail.(string)
				res[i].MaxresThumbnail = &m
			}
			if MediumThumbnail, ok := row1["mediumthumbnail"]; ok {
				m := MediumThumbnail.(string)
				res[i].MediumThumbnail = &m
			}
			if PublishedAt, ok := row1["publishedat"]; ok {
				a := PublishedAt.(time.Time)
				res[i].PublishedAt = &a
			}
			if StandardThumbnail, ok := row1["standardthumbnail"]; ok {
				s := StandardThumbnail.(string)
				res[i].StandardThumbnail = &s
			}
			if Thumbnail, ok := row1["thumbnail"]; ok {
				s := Thumbnail.(string)
				res[i].Thumbnail = &s
			}
			if Title, ok := row1["title"]; ok {
				res[i].Title = Title.(string)
			}
		}
	}
	return res
}

func videoConvert(iter *gocql.Iter) []video.Video {
	res := make([]video.Video, iter.NumRows())
	for i, _ := range res {
		row1 := make(map[string]interface{})
		if !iter.MapScan(row1) {
			break
		} else {
			if Id, ok := row1["id"]; ok {
				res[i].Id = Id.(string)
			}
			if AllowedRegions, ok := row1["allowedregions"]; ok {
				res[i].AllowedRegions = AllowedRegions.([]string)
			}
			if BlockedRegions, ok := row1["blockedregions"]; ok {
				res[i].BlockedRegions = BlockedRegions.([]string)
			}
			if Caption, ok := row1["caption"]; ok {
				res[i].Caption = Caption.(string)
			}
			if CategoryId, ok := row1["categoryid"]; ok {
				res[i].CategoryId = CategoryId.(string)
			}
			if ChannelId, ok := row1["channelid"]; ok {
				res[i].ChannelId = ChannelId.(string)
			}
			if ChannelTitle, ok := row1["channeltitle"]; ok {
				res[i].ChannelTitle = ChannelTitle.(string)
			}
			if DefaultAudioLanguage, ok := row1["defaultaudiolanguage"]; ok {
				res[i].DefaultAudioLanguage = DefaultAudioLanguage.(string)
			}
			if DefaultLanguage, ok := row1["defaultlanguage"]; ok {
				res[i].DefaultLanguage = DefaultLanguage.(string)
			}
			if Definition, ok := row1["definition"]; ok {
				res[i].Definition = Definition.(int)
			}
			if Description, ok := row1["description"]; ok {
				res[i].Description = Description.(string)
			}
			if Dimension, ok := row1["dimension"]; ok {
				res[i].Dimension = Dimension.(string)
			}
			if Duration, ok := row1["duration"]; ok {
				d := Duration.(int)
				res[i].Duration = int64(d)
			}
			if HighThumbnail, ok := row1["highthumbnail"]; ok {
				h := HighThumbnail.(string)
				res[i].HighThumbnail = &h
			}
			if LicensedContent, ok := row1["licensedcontent"]; ok {
				l := LicensedContent.(bool)
				res[i].LicensedContent = &l
			}
			if LiveBroadcastContent, ok := row1["livebroadcastcontent"]; ok {
				res[i].LiveBroadcastContent = LiveBroadcastContent.(string)
			}
			if LiveBroadcastContent, ok := row1["livebroadcastcontent"]; ok {
				res[i].LiveBroadcastContent = LiveBroadcastContent.(string)
			}
			if LocalizedDescription, ok := row1["localizeddescription"]; ok {
				res[i].LocalizedDescription = LocalizedDescription.(string)
			}
			if LocalizedTitle, ok := row1["localizedtitle"]; ok {
				res[i].LocalizedTitle = LocalizedTitle.(string)
			}
			if MaxresThumbnail, ok := row1["maxresthumbnail"]; ok {
				m := MaxresThumbnail.(string)
				res[i].MaxresThumbnail = &m
			}
			if MediumThumbnail, ok := row1["mediumthumbnail"]; ok {
				m := MediumThumbnail.(string)
				res[i].MediumThumbnail = &m
			}
			if Projection, ok := row1["projection"]; ok {
				res[i].Projection = Projection.(string)
			}
			if PublishedAt, ok := row1["publishedat"]; ok {
				a := PublishedAt.(time.Time)
				res[i].PublishedAt = &a
			}
			if StandardThumbnail, ok := row1["standardthumbnail"]; ok {
				s := StandardThumbnail.(string)
				res[i].StandardThumbnail = &s
			}
			if Tags, ok := row1["tags"]; ok {
				res[i].Tags = Tags.([]string)
			}
			if Thumbnail, ok := row1["thumbnail"]; ok {
				t := Thumbnail.(string)
				res[i].Thumbnail = &t
			}
			if Title, ok := row1["title"]; ok {
				res[i].Title = Title.(string)
			}
		}
	}
	return res
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

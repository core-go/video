package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/core-go/video"
	"github.com/core-go/video/category"
	"github.com/lib/pq"
)

type PostgreVideoService struct {
	db                  *sql.DB
	tubeCategory        category.CategorySyncClient
	channelFields  		map[string]int
	modelTypeChannel 	reflect.Type
	playlistFields 		map[string]int
	videoFields    		map[string]int
	categoryFields 		map[string]int
}

func NewPostgreVideoService(db *sql.DB, tubeCategory category.CategorySyncClient) (*PostgreVideoService, error) {
	var resChannel []video.Channel
	modelTypeChannel := reflect.TypeOf(resChannel).Elem()
	channelFields, er1 := GetColumnIndexes(modelTypeChannel)
	if er1 != nil {
		return nil, er1
	}

	var resPlaylist []video.Playlist
	modelTypePlaylist := reflect.TypeOf(resPlaylist).Elem()
	playlistFields, er2 := GetColumnIndexes(modelTypePlaylist)
	if er2 != nil {
		return nil, er2
	}

	var resCategory video.Categories
	modelTypeCategory := reflect.TypeOf(resCategory)
	categoryFields, er3 := GetColumnIndexes(modelTypeCategory)
	if er3 != nil {
		return nil, er3
	}

	var resVideo []video.Video
	modelTypeVideo := reflect.TypeOf(resVideo).Elem()
	videoFields, er4 := GetColumnIndexes(modelTypeVideo)
	if er4 != nil {
		return nil, er4
	}

	return &PostgreVideoService{
		db:             db,
		tubeCategory:   tubeCategory,
		channelFields:  channelFields,
		modelTypeChannel: modelTypeChannel,
		playlistFields: playlistFields,
		videoFields:    videoFields,
		categoryFields: categoryFields,
	}, nil
}

func (s *PostgreVideoService) GetChannel(ctx context.Context, channelId string, fields []string) (*video.Channel, error) {
	if len(fields) == 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from channel where id = $1`, strings.Join(fields, ","))
	var arrRes []video.Channel
	err := QueryWithMapAndArray(ctx, s.db, s.channelFields, &arrRes, pq.Array, query, channelId)
	if err != nil {
		return nil, err
	}
	if len(arrRes) != 0 && len(arrRes[0].ChannelList) > 0 {
		channels, err := s.GetChannels(ctx, arrRes[0].ChannelList, []string{})
		if err != nil {
			return nil, err
		}
		arrRes[0].Channels = *channels
	}
	if len(arrRes) == 0 {
		return nil, nil
	}
	return &arrRes[0], nil
}

func (s *PostgreVideoService) GetChannels(ctx context.Context, ids []string, fields []string) (*[]video.Channel, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = fmt.Sprintf(`$%d`, i+1)
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from channel where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var arrRes []video.Channel
	err := QueryWithMapAndArray(ctx, s.db, s.channelFields, &arrRes, pq.Array, query, cc...)
	if err != nil {
		return nil, err
	}
	if len(arrRes) == 0 {
		return nil, nil
	}
	return &arrRes, nil
}

func (s *PostgreVideoService) GetPlaylist(ctx context.Context, id string, fields []string) (*video.Playlist, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from playlist where id = $1`, strings.Join(fields, ","))
	var res []video.Playlist
	err := QueryWithMap(ctx, s.db, s.playlistFields, &res, query, id)
	if err != nil {
		return nil, err
	}
	return &res[0], nil
}

func (s *PostgreVideoService) GetPlaylists(ctx context.Context, ids []string, fields []string) (*[]video.Playlist, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = fmt.Sprintf(`$%d`, i+1)
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from playlist where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var res []video.Playlist
	err := QueryWithMap(ctx, s.db, s.playlistFields, &res, query, cc...)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (s *PostgreVideoService) GetVideo(ctx context.Context, id string, fields []string) (*video.Video, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from video where id = $1`, strings.Join(fields, ","))
	var arrRes []video.Video
	err := QueryWithMapAndArray(ctx, s.db, s.videoFields, &arrRes, pq.Array, query, id)
	if err != nil {
		return nil, err
	}
	if len(arrRes) == 0 {
		return nil, nil
	}
	return &arrRes[0], nil
}

func (s *PostgreVideoService) GetVideos(ctx context.Context, ids []string, fields []string) (*[]video.Video, error) {
	question := make([]string, len(ids))
	cc := make([]interface{}, len(ids))
	for i, v := range ids {
		question[i] = fmt.Sprintf(`$%d`, i+1)
		cc[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from video where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var arrRes []video.Video
	err := QueryWithMapAndArray(ctx, s.db, s.videoFields, &arrRes, pq.Array, query, cc...)
	if err != nil {
		return nil, err
	}
	if len(arrRes) == 0 {
		return nil, nil
	}
	return &arrRes, nil
}

func (s *PostgreVideoService) GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	query := fmt.Sprintf(`select %s from playlist where channelId=$1 order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), max, next)
	var res video.ListResultPlaylist
	er1 := QueryWithMap(ctx, s.db, s.playlistFields, &res.List, query, channelId)
	if er1 != nil {
		return nil, er1
	}
	res.Limit = max
	lenList := len(res.List)
	res.Total = lenList
	r, er2 := strconv.Atoi(next)
	if er2 != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")

	return &res, nil
}

func (s *PostgreVideoService) GetChannelVideos(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	query := fmt.Sprintf(`select %s from video where channelId=$1 order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), max, next)
	var res video.ListResultVideos
	er1 := QueryWithMapAndArray(ctx, s.db, s.videoFields, &res.List, pq.Array, query, channelId)
	if er1 != nil {
		return nil, er1
	}
	res.Limit = max
	lenList := len(res.List)
	res.Total = lenList
	r, er2 := strconv.Atoi(next)
	if er2 != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")

	return &res, nil
}

func (s *PostgreVideoService) GetPlaylistVideos(ctx context.Context, playlistId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	query1 := `select * from playlistVideo where id = $1 `
	var resPlaylistVideoIdVideos []video.PlaylistVideoIdVideos
	er1 := QueryWithMapAndArray(ctx, s.db, nil, &resPlaylistVideoIdVideos, pq.Array, query1, playlistId)
	if er1 != nil {
		return nil, er1
	}
	questions := make([]string, len(resPlaylistVideoIdVideos[0].Videos))
	values := make([]interface{}, len(resPlaylistVideoIdVideos[0].Videos))
	for i, v := range resPlaylistVideoIdVideos[0].Videos {
		questions[i] = fmt.Sprintf(`$%d`, i+1)
		values[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	query2 := fmt.Sprintf(`select %s from video where id in (%s) order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), strings.Join(questions, ","), max, next)
	var res video.ListResultVideos
	er2 := QueryWithMapAndArray(ctx, s.db, s.videoFields, &res.List, pq.Array, query2, values...)
	if er2 != nil {
		return nil, er2
	}
	lenList := len(res.List)
	res.Total = lenList
	res.Limit = max
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("invalid nextPageToken")
	}
	res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")
	return &res, nil
}

func (s *PostgreVideoService) GetCategories(ctx context.Context, regionCode string) (*video.Categories, error) {
	sql := `select * from category where id = $1`
	var arrCategory []video.Categories
	err := QueryWithMapAndArray(ctx, s.db, s.categoryFields, &arrCategory, pq.Array,sql , regionCode)
	if err != nil {
		return nil, err
	}
	category := arrCategory[0]
	if category.Data == nil {
		res, er1 := s.tubeCategory.GetCagetories(regionCode)
		if er1 != nil {
			return nil, er1
		}
		query := "insert into category (id,data) values ($1, $2)"
		_, err = s.db.Exec(query, regionCode, pq.Array(*res))
		if err != nil {
			return nil, err
		}
		result := video.Categories{
			Id:   regionCode,
			Data: *res,
		}
		return &result, nil
	}
	return &category, nil
}

func (s *PostgreVideoService) SearchChannel(ctx context.Context, channelSM video.ChannelSM, max int, nextPageToken string, fields []string) (*video.ListResultChannel, error) {
	next := getNext(nextPageToken)
	query, statement := buildChannelQuery(channelSM, fields)
	query = query + fmt.Sprintf(` limit %d offset %s`, max, next)
	var listResultChannel video.ListResultChannel
	err := QueryWithMapAndArray(ctx, s.db, s.channelFields, &listResultChannel.List, pq.Array, query, statement...)
	if err != nil {
		return nil, err
	}
	listResultChannel.Limit = max
	lenList := len(listResultChannel.List)
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("invalid nextPageToken")
	}
	if lenList > 0 {
		listResultChannel.NextPageToken = createNextPageToken(lenList, max, r, listResultChannel.List[lenList-1].Id, "")
	}
	return &listResultChannel, nil
}

func (s *PostgreVideoService) SearchPlaylists(ctx context.Context, playlistSM video.PlaylistSM, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	next := getNext(nextPageToken)
	query, statement := buildPlaylistQuery(playlistSM, fields)
	query = query + fmt.Sprintf(` limit %d offset %s`, max, next)
	var res video.ListResultPlaylist
	err := QueryWithMap(ctx, s.db, s.playlistFields, &res.List, query, statement...)
	if err != nil {
		return nil, err
	}
	lenList := len(res.List)
	res.Limit = max
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	if lenList > 0 {
		res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")
	}

	return &res, nil
}

func (s *PostgreVideoService) SearchVideos(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	next := getNext(nextPageToken)
	query, statement := buildVideoQuery(itemSM, fields)
	query = query + fmt.Sprintf(` limit %d offset %s`, max, next)
	var res video.ListResultVideos
	err := QueryWithMapAndArray(ctx, s.db, s.videoFields, &res.List, pq.Array, query, statement...)
	if err != nil {
		return nil, err
	}
	lenList := len(res.List)
	res.Limit = max
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	if lenList > 0 {
		res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")
	}
	return &res, nil
}

func (s *PostgreVideoService) Search(ctx context.Context, itemSM video.ItemSM, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	queryChannel, statementChannel := buildSearchUnionQuery("channel", itemSM, fields)
	var channels []video.Video
	var resChannel video.ListResultVideos
	er1 := QueryWithMap(ctx, s.db, s.videoFields, &channels, queryChannel, statementChannel...)
	if er1 != nil {
		return nil, er1
	}
	resChannel.List = channels
	resChannelList := resChannel.List

	// ----------------------
	queryPlaylist, statementPlaylist := buildSearchUnionQuery("playlist", itemSM, fields)
	var playlists []video.Video
	var resPlaylist video.ListResultVideos
	er2 := QueryWithMap(ctx, s.db, s.videoFields, &playlists, queryPlaylist, statementPlaylist...)
	if er2 != nil {
		return nil, er2
	}
	resPlaylist.List = playlists
	resPlaylistList := resPlaylist.List

	// ----------------------
	queryVideo, statementVideo := buildSearchUnionQuery("video", itemSM, fields)
	var videos []video.Video
	var resVideo video.ListResultVideos
	er3 := QueryWithMap(ctx, s.db, s.videoFields, &videos, queryVideo, statementVideo...)
	if er3 != nil {
		return nil, er3
	}
	resVideo.List = videos
	resVideoList := resVideo.List
	var result, combine1 []video.Video
	combine1 = append(resPlaylistList, resVideoList...)
	result = append(resChannelList, combine1...)
	var res video.ListResultVideos
	res.List = result
	lenList := len(res.List)
	res.Limit = max
	next := getNext(nextPageToken)
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	if lenList > 0 {
		res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")
	}
	return &res, nil
}

func (s *PostgreVideoService) GetRelatedVideos(ctx context.Context, videoId string, max int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	next := getNext(nextPageToken)
	var a []string
	resVd, err := s.GetVideo(ctx, videoId, a)
	if err != nil {
		return nil, err
	}
	var result video.ListResultVideos
	if resVd == nil {
		return nil, errors.New("video doesn't exist")
	} else {
		if len(resVd.Tags) == 0 {
			return nil, errors.New("video doesn't have any tag")
		} else {
			query, statement := buildRelatedVideoQuery(videoId, resVd.Tags, fields)
			query = query + fmt.Sprintf(` limit %d offset %s`, max, next)
			var arrRes []video.Video
			err := QueryWithMapAndArray(ctx, s.db, s.videoFields, &arrRes, pq.Array, query, statement...)
			if err != nil {
				return nil, err
			}
			result.List = arrRes
			lenList := len(arrRes)
			if lenList == 0 {
				return nil, errors.New("there is no related video")
			}
			result.List = arrRes
			result.Limit = max
			r, err := strconv.Atoi(next)
			if err != nil {
				return nil, errors.New("nextPageToken wrong")
			}
			if lenList > 0 {
				result.NextPageToken = createNextPageToken(lenList, max, r, result.List[lenList-1].Id, "")
			}
		}
	}

	return &result, nil
}

func (s *PostgreVideoService) GetPopularVideos(ctx context.Context, regionCode string, categoryId string, limit int, nextPageToken string, fields []string) (*video.ListResultVideos, error) {
	next := getNext(nextPageToken)
	query, statement := buildPopularVideoQuery(regionCode, categoryId, fields)
	query = query + fmt.Sprintf(` limit %d offset %s`, limit, next)
	var videos []video.Video
	err := QueryWithMapAndArray(ctx, s.db, s.videoFields, &videos, pq.Array, query, statement...)
	if err != nil {
		return nil, err
	}
	var res video.ListResultVideos
	res.List = videos
	lenList := len(videos)
	res.Limit = limit
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("invalid nextPageToken")
	}
	if lenList > 0 {
		res.NextPageToken = createNextPageToken(lenList, limit, r, res.List[lenList-1].Id, "")
	}

	return &res, nil
}

func getNext(nextPageToken string) (next string) {
	if len(nextPageToken) > 0 {
		next = strings.Split(nextPageToken, "|")[0]
	} else {
		next = "0"
	}
	return
}

func createNextPageToken(lenList int, limit int, skip int, id string, name string) string {
	if len(name) <= 0 {
		name = "id"
	}
	if lenList < limit {
		return ""
	} else {
		if lenList > 0 {
			return fmt.Sprintf(`%d|%s`, skip+limit, id)
		} else {
			return ""
		}
	}
}

func buildChannelQuery(s video.ChannelSM, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from channel`, strings.Join(fields, ","))
	var condition []string
	var params []interface{}
	i := 1
	if len(s.ChannelId) > 0 {
		params = append(params, s.ChannelId)
		condition = append(condition, fmt.Sprintf(`id = $%d`, i))
		i++
	}
	if len(s.RegionCode) > 0 {
		params = append(params, s.RegionCode)
		condition = append(condition, fmt.Sprintf(`country = $%d`, i))
		i++
	}
	if s.PublishedAfter != nil {
		params = append(params, s.PublishedAfter)
		condition = append(condition, fmt.Sprintf(`publishedAt <= $%d`, i))
		i++
	}
	if s.PublishedBefore != nil {
		params = append(params, s.PublishedBefore)
		condition = append(condition, fmt.Sprintf(`publishedAt > $%d`, i))
		i++
	}
	if len(s.Q) > 0 {
		q := "%" + s.Q + "%"
		params = append(params, q, q)
		condition = append(condition, fmt.Sprintf(`(title ilike $%d or description ilike $%d)`, i, i+1))
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		query += fmt.Sprintf(` where %s`, cond)
	}
	if len(s.Sort) > 0 {
		query += fmt.Sprintf(` order by %s desc`, s.Sort)
	}
	return query, params
}

func buildPlaylistQuery(s video.PlaylistSM, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}

	query := fmt.Sprintf(`select %s from playlist`, strings.Join(fields, ","))
	var condition []string
	var params []interface{}
	i := 1

	if len(s.ChannelId) > 0 {
		params = append(params, s.ChannelId)
		condition = append(condition, fmt.Sprintf(`channelid = $%d`, i))
		i++
	}
	if s.PublishedAfter != nil {
		params = append(params, s.PublishedAfter)
		condition = append(condition, fmt.Sprintf(`publishedAt <= $%d`, i))
		i++
	}
	if s.PublishedBefore != nil {
		params = append(params, s.PublishedBefore)
		condition = append(condition, fmt.Sprintf(`publishedAt > $%d`, i))
		i++
	}
	if len(s.Q) > 0 {
		q := "%" + s.Q + "%"
		params = append(params, q, q)
		condition = append(condition, fmt.Sprintf(`(title ilike $%d or description ilike $%d)`, i, i+1))
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		query += fmt.Sprintf(` where %s`, cond)
	}

	if len(s.Sort) > 0 {
		query += fmt.Sprintf(` order by %s desc`, s.Sort)
	}

	return query, params
}

func buildVideoQuery(s video.ItemSM, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	query := fmt.Sprintf(`select %s from video`, strings.Join(fields, ","))
	var condition []string
	var params []interface{}
	i := 1

	if len(s.ChannelId) > 0 {
		params = append(params, s.ChannelId)
		condition = append(condition, fmt.Sprintf(`channelid = $%d`, i))
		i++
	}
	if s.PublishedAfter != nil {
		params = append(params, s.PublishedAfter)
		condition = append(condition, fmt.Sprintf(`publishedAt <= $%d`, i))
		i++
	}
	if s.PublishedBefore != nil {
		params = append(params, s.PublishedBefore)
		condition = append(condition, fmt.Sprintf(`publishedAt > $%d`, i))
		i++
	}
	if len(s.RegionCode) > 0 {
		params = append(params, s.RegionCode)
		condition = append(condition, fmt.Sprintf(`(blockedRegions is null or $%d != all(blockedRegions))`, i))
		//https://popsql.com/learn-sql/postgresql/how-to-query-arrays-in-postgresql
		i++
	}
	if len(s.Q) > 0 {
		q := "%" + s.Q + "%"
		params = append(params, q, q)
		condition = append(condition, fmt.Sprintf(`(title ilike $%d or description ilike $%d)`, i, i+1))
	}
	if len(s.Duration) > 0 {
		var compare string
		switch s.Duration {
		case "short":
			compare = "duration between 1 and 240"
		case "medium":
			compare = "duration between 241 and 1200"
		case "long":
			compare = "duration > 1200"
		default:
			compare = ""
		}
		if len(compare) > 0 {
			condition = append(condition, compare)
		}
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		query += fmt.Sprintf(` where %s`, cond)
	}

	if len(s.Sort) > 0 {
		query += fmt.Sprintf(` order by %s desc`, s.Sort)
	}

	return query, params
}

func buildRelatedVideoQuery(videoId string, tags []string, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}

	var finalsql string
	var condition []string
	var params []interface{}
	i := 1

	finalsql = fmt.Sprintf(`select %s from video`, strings.Join(fields, ","))

	if len(tags) == 1 {
		finalsql += fmt.Sprintf(` where id <> $%d and $%d = any (tags)`, i, i+1)
		params = append(params, videoId, tags[0])
	} else {
		finalsql += fmt.Sprintf(` where id <> $%d`, i)
		params = append(params, videoId)
		i++
		for _, value := range tags {
			params = append(params, value)
			condition = append(condition, fmt.Sprintf(`$%d = any (tags)`, i))
			i++
		}
		if len(condition) > 0 {
			cond := strings.Join(condition, " or ")
			finalsql += fmt.Sprintf(` and (%s)`, cond)
		}
	}

	return finalsql, params
}

func buildPopularVideoQuery(regionCode string, categoryId string, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}

	query := fmt.Sprintf(`select %s from video`, strings.Join(fields, ","))
	var condition []string
	var params []interface{}
	i := 1

	if len(categoryId) > 0 {
		params = append(params, categoryId)
		condition = append(condition, fmt.Sprintf(`categoryId = $%d`, i))
		i++
	}
	if len(regionCode) > 0 {
		params = append(params, regionCode)
		condition = append(condition, fmt.Sprintf(`blockedRegions is null or $%d <> ALL (blockedRegions)`, i))
		i++
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		query += fmt.Sprintf(` where %s`, cond)
	}

	return query, params
}

func buildSearchUnionQuery(searchType string, s video.ItemSM, fields []string) (string, []interface{}) {
	channelString := `id, title, description, publishedAt`
	playlistString := `id, channelId, channelTitle, title, description, count, publishedAt`
	videoString := "id, channelId, channelTitle, title, description, duration, publishedAt"

	if len(fields) <= 0 && searchType == "channel" {
		fields = append(fields, channelString)
	} else if len(fields) <= 0 && searchType == "playlist" {
		fields = append(fields, playlistString)
	} else if len(fields) <= 0 && searchType == "video" {
		fields = append(fields, videoString)
	}

	query := fmt.Sprintf(`select %s from %s`, strings.Join(fields, ","), searchType)
	var condition []string
	var params []interface{}
	i := 1

	if len(s.Q) > 0 {
		q := "%" + s.Q + "%"
		params = append(params, q, q)
		condition = append(condition, fmt.Sprintf(`title ilike $%d or description ilike $%d`, i, i+1))
		i++
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		query += fmt.Sprintf(` where %s`, cond)
	}

	return query, params
}

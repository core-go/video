package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	pgsql "github.com/core-go/sql"
	"github.com/core-go/video"
	"github.com/core-go/video/category"
	"github.com/lib/pq"
)

type PostgreVideoService struct {
	db                  *sql.DB
	tubeCategory        category.CategorySyncClient
	fieldsIndexChannel  map[string]int
	fieldsIndexPlaylist map[string]int
	fieldsIndexVideo    map[string]int
	fieldsIndexCategory map[string]int
}

func NewPostgreVideoService(db *sql.DB, tubeCategory category.CategorySyncClient) (*PostgreVideoService, error) {
	var resChannel []video.Channel
	modelTypeChannel := reflect.TypeOf(resChannel).Elem()
	fieldsIndexChannel, er1 := pgsql.GetColumnIndexes(modelTypeChannel)
	if er1 != nil {
		return nil, er1
	}

	var resPlaylist []video.Playlist
	modelTypePlaylist := reflect.TypeOf(resPlaylist).Elem()
	fieldsIndexPlaylist, er2 := pgsql.GetColumnIndexes(modelTypePlaylist)
	if er2 != nil {
		return nil, er2
	}

	var resCategory video.Categories
	modelTypeCategory := reflect.TypeOf(resCategory)
	fieldsIndexCategory, er3 := pgsql.GetColumnIndexes(modelTypeCategory)
	if er3 != nil {
		return nil, er3
	}

	var resVideo []video.Video
	modelTypeVideo := reflect.TypeOf(resVideo).Elem()
	fieldsIndexVideo, er4 := pgsql.GetColumnIndexes(modelTypeVideo)
	if er4 != nil {
		return nil, er4
	}

	return &PostgreVideoService{
		db:                  db,
		tubeCategory:        tubeCategory,
		fieldsIndexChannel:  fieldsIndexChannel,
		fieldsIndexPlaylist: fieldsIndexPlaylist,
		fieldsIndexVideo:    fieldsIndexVideo,
		fieldsIndexCategory: fieldsIndexCategory,
	}, nil
}

func (s *PostgreVideoService) GetChannel(ctx context.Context, channelId string, fields []string) (*video.Channel, error) {
	if len(fields) == 0 {
		fields = append(fields, "*")
	}
	strq := fmt.Sprintf(`select %s from channel where id = $1`, strings.Join(fields, ","))
	query, err := s.db.QueryContext(ctx, strq, channelId)
	if err != nil {
		return nil, err
	}
	res, err := channelResult(query)
	if err != nil {
		return nil, err
	}
	if len(res) != 0 && len(res[0].ChannelList) > 0 {
		channels, err := s.GetChannels(ctx, res[0].ChannelList, []string{})
		if err != nil {
			return nil, err
		}
		res[0].Channels = *channels
	}
	if len(res) == 0 {
		return nil, nil
	}
	return &res[0], nil
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
	strq := fmt.Sprintf(`Select %s from channel where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	query, err := s.db.Query(strq, cc...)
	if err != nil {
		return nil, err
	}
	res, err := channelResult(query)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (s *PostgreVideoService) GetPlaylist(ctx context.Context, id string, fields []string) (*video.Playlist, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	strq := fmt.Sprintf(`Select %s from playlist where id = $1`, strings.Join(fields, ","))
	var res []video.Playlist
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexPlaylist, &res, strq, id)
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
	strq := fmt.Sprintf(`Select %s from playlist where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	var res []video.Playlist
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexPlaylist, &res, strq, cc...)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (s *PostgreVideoService) GetVideo(ctx context.Context, id string, fields []string) (*video.Video, error) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	strq := fmt.Sprintf(`Select %s from video where id = $1`, strings.Join(fields, ","))
	query, err := s.db.Query(strq, id)
	if err != nil {
		return nil, err
	}
	res, err := videoResult(query)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return &res[0], nil
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
	strq := fmt.Sprintf(`Select %s from video where id in (%s)`, strings.Join(fields, ","), strings.Join(question, ","))
	query, err := s.db.Query(strq, cc...)
	if err != nil {
		return nil, err
	}
	res, err := videoResult(query)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (s *PostgreVideoService) GetChannelPlaylists(ctx context.Context, channelId string, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	fmt.Println("GetChannelPlaylists")
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	strq := fmt.Sprintf(`select %s from playlist where channelId=$1 order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), max, next)
	var res video.ListResultPlaylist
	er1 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexPlaylist, &res.List, strq, channelId)
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
	// ---- none
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	strq := fmt.Sprintf(`select %s from video where channelId=$1 order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), max, next)
	var res video.ListResultVideos
	er1 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &res.List, strq, channelId)
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
	fmt.Println("GetPlaylistVideos")
	strq1 := `select videos from playlistVideo where id = $1 `
	var resVideoIds []string
	er1 := pgsql.QueryWithMap(ctx, s.db, nil, &resVideoIds, strq1, playlistId)
	if er1 != nil {
		return nil, er1
	}
	questions := make([]string, len(resVideoIds))
	values := make([]interface{}, len(resVideoIds))
	for i, v := range resVideoIds {
		questions[i] = fmt.Sprintf(`$%d`, i+1)
		values[i] = v
	}
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}
	next := getNext(nextPageToken)
	strq2 := fmt.Sprintf(`select %s from video where id in (%s) order by publishedAt desc limit %d offset %s`, strings.Join(fields, ","), strings.Join(questions, ","), max, next)
	var resVideos []video.Video
	er2 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &resVideos, strq2, values...)
	if er2 != nil {
		return nil, er2
	}
	var res video.ListResultVideos
	res.List = resVideos
	lenList := len(res.List)
	res.Total = lenList
	res.Limit = max
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	res.NextPageToken = createNextPageToken(lenList, max, r, res.List[lenList-1].Id, "")
	return &res, nil
}

func (s *PostgreVideoService) GetCategories(ctx context.Context, regionCode string) (*video.Categories, error) {
	sql := `select * from category where id = $1`
	query, err := s.db.Query(sql, regionCode)
	if err != nil {
		return nil, err
	}
	var category video.Categories
	for query.Next() {
		query.Scan(&category.Id, pq.Array(&category.Data))
	}
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
	strq, statement := buildChannelQuery(channelSM, fields)
	strq = strq + fmt.Sprintf(` limit %d offset %s`, max, next)
	var resultChannel []video.Channel
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexChannel, &resultChannel, strq, statement...)
	if err != nil {
		return nil, err
	}
	var listResultChannel video.ListResultChannel
	listResultChannel.List = resultChannel
	listResultChannel.Limit = max
	lenList := len(resultChannel)
	listResultChannel.Total = lenList
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	if lenList > 0 {
		listResultChannel.NextPageToken = createNextPageToken(lenList, max, r, listResultChannel.List[lenList-1].Id, "")
	}
	return &listResultChannel, nil
}

func (s *PostgreVideoService) SearchPlaylists(ctx context.Context, playlistSM video.PlaylistSM, max int, nextPageToken string, fields []string) (*video.ListResultPlaylist, error) {
	next := getNext(nextPageToken)
	strq, statement := buildPlaylistQuery(playlistSM, fields)
	strq = strq + fmt.Sprintf(` limit %d offset %s`, max, next)
	var playlists []video.Playlist
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexPlaylist, &playlists, strq, statement...)
	if err != nil {
		return nil, err
	}
	var res video.ListResultPlaylist
	res.List = playlists
	lenList := len(playlists)
	res.Total = lenList
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
	strq, statement := buildVideoQuery(itemSM, fields)
	strq = strq + fmt.Sprintf(` limit %d offset %s`, max, next)
	var videos []video.Video
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &videos, strq, statement...)
	if err != nil {
		return nil, err
	}
	var res video.ListResultVideos
	res.List = videos
	lenList := len(videos)
	res.Total = lenList
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
	strqChannel, statementChannel := buildSearchUnionQuery("channel", itemSM, fields)
	var channels []video.Video
	var resChannel video.ListResultVideos
	er1 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &channels, strqChannel, statementChannel...)
	if er1 != nil {
		return nil, er1
	}
	resChannel.List = channels
	resChannelList := resChannel.List

	// ----------------------
	strqPlaylist, statementPlaylist := buildSearchUnionQuery("playlist", itemSM, fields)
	var playlists []video.Video
	var resPlaylist video.ListResultVideos
	er2 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &playlists, strqPlaylist, statementPlaylist...)
	if er2 != nil {
		return nil, er2
	}
	resPlaylist.List = playlists
	resPlaylistList := resPlaylist.List

	// ----------------------
	strqVideo, statementVideo := buildSearchUnionQuery("video", itemSM, fields)
	var videos []video.Video
	var resVideo video.ListResultVideos
	er3 := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &videos, strqVideo, statementVideo...)
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
	res.Total = lenList
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
			strq, statement := buildRelatedVideoQuery(videoId, resVd.Tags, fields)
			strq = strq + fmt.Sprintf(` limit %d offset %s`, max, next)
			rows, err := s.db.Query(strq, statement...)
			if err != nil {
				return nil, err
			}
			videos, err := videoResult(rows)
			if err != nil {
				return nil, err
			}
			result.List = videos
			lenList := len(videos)
			if lenList == 0 {
				return nil, errors.New("there is no related video")
			}
			result.List = videos
			result.Total = lenList
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
	strq, statement := buildPopularVideoQuery(regionCode, categoryId, fields)
	strq = strq + fmt.Sprintf(` limit %d offset %s`, limit, next)
	var videos []video.Video
	err := pgsql.QueryWithMap(ctx, s.db, s.fieldsIndexVideo, &videos, strq, statement...)
	if err != nil {
		return nil, err
	}
	var res video.ListResultVideos
	res.List = videos
	lenList := len(videos)
	res.Total = lenList
	res.Limit = limit
	r, err := strconv.Atoi(next)
	if err != nil {
		return nil, errors.New("nextPageToken wrong")
	}
	if lenList > 0 {
		res.NextPageToken = createNextPageToken(lenList, limit, r, res.List[lenList-1].Id, "")
	}

	return &res, nil
}

func channelResult(query *sql.Rows) ([]video.Channel, error) {
	var res []video.Channel
	channel := video.Channel{}
	for query.Next() {
		err := query.Scan(&channel.Id, &channel.Count, &channel.Country, &channel.CustomUrl, &channel.Description, &channel.Favorites, &channel.HighThumbnail, &channel.ItemCount, &channel.Likes, &channel.LocalizedDescription, &channel.LocalizedTitle, &channel.MediumThumbnail, &channel.PlaylistCount, &channel.PlaylistItemCount, &channel.PlaylistVideoCount, &channel.PlaylistVideoItemCount, &channel.PublishedAt, &channel.Thumbnail, &channel.LastUpload, &channel.Title, &channel.Uploads, pq.Array(&channel.ChannelList))
		if err != nil {
			return nil, err
		}
		res = append(res, channel)
	}
	return res, nil
}

func playlistResult(query *sql.Rows) ([]video.Playlist, error) {
	var res []video.Playlist
	var playlist video.Playlist
	for query.Next() {
		err := query.Scan(&playlist.Id, &playlist.ChannelId, &playlist.ChannelTitle, &playlist.Count, &playlist.ItemCount, &playlist.Description, &playlist.HighThumbnail, &playlist.LocalizedDescription, &playlist.LocalizedTitle, &playlist.MaxresThumbnail, &playlist.MediumThumbnail, &playlist.PublishedAt, &playlist.StandardThumbnail, &playlist.Thumbnail, &playlist.Title)
		if err != nil {
			return nil, err
		}
		res = append(res, playlist)
	}
	return res, nil
}

func videoResult(query *sql.Rows) ([]video.Video, error) {
	var res []video.Video
	var video video.Video
	for query.Next() {
		err := query.Scan(&video.Id, &video.Caption, &video.CategoryId, &video.ChannelId, &video.ChannelTitle, &video.DefaultAudioLanguage, &video.DefaultLanguage, &video.Definition, &video.Description, &video.Dimension, &video.Duration, &video.HighThumbnail, &video.LicensedContent, &video.LiveBroadcastContent, &video.LocalizedDescription, &video.LocalizedTitle, &video.MaxresThumbnail, &video.MediumThumbnail, &video.Projection, &video.PublishedAt, &video.StandardThumbnail, pq.Array(&video.Tags), &video.Thumbnail, &video.Title, pq.Array(&video.BlockedRegions), pq.Array(&video.AllowedRegions))
		if err != nil {
			return nil, err
		}
		res = append(res, video)
	}
	return res, nil
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
	strq := fmt.Sprintf(`select %s from channel`, strings.Join(fields, ","))
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
		strq += fmt.Sprintf(` where %s`, cond)
	}
	if len(s.Sort) > 0 {
		strq += fmt.Sprintf(` order by %s desc`, s.Sort)
	}
	return strq, params
}

func buildPlaylistQuery(s video.PlaylistSM, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}

	strq := fmt.Sprintf(`select %s from playlist`, strings.Join(fields, ","))
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
		condition = append(condition, fmt.Sprintf(`publishedAt > $%d`, i))
		i++
	}
	if s.PublishedBefore != nil {
		params = append(params, s.PublishedBefore)
		condition = append(condition, fmt.Sprintf(`publishedAt <= $%d`, i))
		i++
	}
	if len(s.Q) > 0 {
		q := "%" + s.Q + "%"
		params = append(params, q, q)
		condition = append(condition, fmt.Sprintf(`(title ilike $%d or description ilike $%d)`, i, i+1))
	}

	// ---- đổ các item trong []condition và chuỗi cond (nếu []condiion ko empty), từ đó nối vào câu strq
	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		strq += fmt.Sprintf(` where %s`, cond)
	}

	// ---- nếu parameter sort trong request ko empty thì nối thêm điều kiện sort vào câu strq
	if len(s.Sort) > 0 {
		strq += fmt.Sprintf(` order by %s desc`, s.Sort)
	}

	// ---- return ra câu query cuối cùng và tập hợp các params
	return strq, params
}

func buildVideoQuery(s video.ItemSM, fields []string) (string, []interface{}) {
	if len(fields) <= 0 {
		fields = append(fields, "*")
	}

	strq := fmt.Sprintf(`select %s from video`, strings.Join(fields, ","))
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
		condition = append(condition, fmt.Sprintf(`publishedAt > $%d`, i))
		i++
	}
	if s.PublishedBefore != nil {
		params = append(params, s.PublishedBefore)
		condition = append(condition, fmt.Sprintf(`publishedAt <= $%d`, i))
		i++
	}
	if len(s.RegionCode) > 0 {
		params = append(params, s.RegionCode)
		condition = append(condition, fmt.Sprintf(`$%d = any (allowedRegions)`, i))
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
			compare = "duration > 0"
		}
		condition = append(condition, compare)
	}

	if len(condition) > 0 {
		cond := strings.Join(condition, " and ")
		strq += fmt.Sprintf(` where %s`, cond)
	}

	if len(s.Sort) > 0 {
		strq += fmt.Sprintf(` order by %s desc`, s.Sort)
	}

	return strq, params
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

	strq := fmt.Sprintf(`select %s from video`, strings.Join(fields, ","))
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
		strq += fmt.Sprintf(` where %s`, cond)
	}

	return strq, params
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

	strq := fmt.Sprintf(`select %s from %s`, strings.Join(fields, ","), searchType)
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
		strq += fmt.Sprintf(` where %s`, cond)
	}

	return strq, params
}

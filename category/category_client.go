package category

import (
	"encoding/json"
	"fmt"
	"github.com/core-go/video"
	"io/ioutil"
	"net/http"
)

type CategorySyncClient struct {
	Key string
}

func NewCategorySyncService(key string) *CategorySyncClient {
	return &CategorySyncClient{Key: key}
}

func (c *CategorySyncClient) GetCagetories(regionCode string) (*[]video.DataCategory, error) {
	if len(regionCode) <= 0 {
		regionCode = "US"
	}
	url := fmt.Sprintf(`https://www.googleapis.com/youtube/v3/videoCategories?key=%s&regionCode=%s`, c.Key, regionCode)
	res, err := convertCategory(url)
	if err != nil {
		return nil, err
	}
	return res, err
}

func convertCategory(url string) (*[]video.DataCategory, error) {
	resp, er0 := http.Get(url)
	if er0 != nil {
		return nil, er0
	}
	var summary CategoryTubeResponse
	body, er1 := ioutil.ReadAll(resp.Body)
	if er1 != nil {
		return nil, er1
	}
	//log.Println(string(body))
	defer resp.Body.Close()
	er2 := json.Unmarshal(body, &summary)
	if er2 != nil {
		return nil, er2
	}
	var categories []video.DataCategory
	for _, v := range summary.Items {
		var category video.DataCategory
		category.Id = v.Id
		category.ChannelId = v.Snippet.ChannelId
		category.Title = v.Snippet.Title
		category.Assignable = v.Snippet.Assignable
		categories = append(categories, category)
	}
	return &categories, nil
}

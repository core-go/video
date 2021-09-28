package video

type CategoryClient interface {
	GetCategories(regionCode string) (*[]DataCategory, error)
}

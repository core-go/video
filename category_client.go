package video

type CategoryClient interface {
	GetCagetories(regionCode string) (*[]DataCategory, error)
}

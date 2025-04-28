package gleaner

import "io"

type CrawlStorage interface {
	store(string, io.Reader) error
	get(string) (io.ReadCloser, error)
	exists(string) (bool, error)
}

type DiscardCrawlStorage struct {
}

func (DiscardCrawlStorage) store(string, io.Reader) error {
	return nil
}
func (DiscardCrawlStorage) get(string) (io.ReadCloser, error) {
	return nil, nil
}
func (DiscardCrawlStorage) exists(string) (bool, error) {
	return false, nil
}

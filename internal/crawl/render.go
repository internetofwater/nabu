package crawl

import (
	"context"
	"io"
	"net/http"

	"github.com/chromedp/chromedp"
	log "github.com/sirupsen/logrus"
)

type SplashClient struct {
	endpoint   string
	httpClient *http.Client
}

func NewSplashClient(endpoint string, httpClient *http.Client) *SplashClient {
	return &SplashClient{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
}

func (r *SplashClient) render(ctx context.Context, url string) (*http.Response, error) {

	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	// run task list
	var res string
	err := chromedp.Run(ctx,
		chromedp.Navigate(`https://pkg.go.dev/time`),
		chromedp.Text(`.Documentation-overview`, &res, chromedp.NodeVisible),
	)
	if err != nil {
		log.Fatal(err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, renderedUrl, nil)
	if err != nil {
		return nil, err
	}
	return r.httpClient.Do(req)
}

func (r *SplashClient) RenderContent(ctx context.Context, url string) ([]byte, error) {
	res, err := r.render(ctx, url)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(res.Body)
}

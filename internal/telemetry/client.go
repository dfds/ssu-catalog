package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a thin Grafana Cloud Mimir (Prometheus-compatible) HTTP client that
// issues instant queries with optional basic auth.
type Client struct {
	baseURL    string
	user       string
	token      string
	httpClient *http.Client
}

// NewClient builds a Mimir client. user/token may be empty (no basic auth).
func NewClient(baseURL, user, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		user:       user,
		token:      token,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// InstantQuery runs a PromQL instant query and returns the resolved vector.
func (c *Client) InstantQuery(ctx context.Context, query string) ([]Sample, error) {
	endpoint := c.baseURL + "/api/v1/query"
	form := url.Values{"query": {query}}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mimir query returned HTTP %d", resp.StatusCode)
	}

	var pr promResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("decoding mimir response: %w", err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("mimir query failed: %s: %s", pr.ErrorType, pr.Error)
	}

	samples := make([]Sample, 0, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		value, err := sampleValue(r.Value)
		if err != nil {
			continue
		}
		samples = append(samples, Sample{Metric: r.Metric, Value: value})
	}
	return samples, nil
}

// sampleValue extracts the float value from a Prometheus [timestamp, "value"]
// pair.
func sampleValue(v [2]any) (float64, error) {
	s, ok := v[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected value type %T", v[1])
	}
	return strconv.ParseFloat(s, 64)
}

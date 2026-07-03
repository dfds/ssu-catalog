package metadata

import (
	"reflect"
	"testing"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

func TestFrom(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *model.AppMetadata
	}{
		{
			name:        "nil when nothing declared",
			annotations: map[string]string{"app.kubernetes.io/name": "api"},
			want:        nil,
		},
		{
			name:        "description only",
			annotations: map[string]string{annoDescription: "Billing API"},
			want:        &model.AppMetadata{Description: "Billing API"},
		},
		{
			name: "links only, sorted by label",
			annotations: map[string]string{
				annoLinkPrefix + "runbook":   "https://runbook",
				annoLinkPrefix + "dashboard": "https://dash",
			},
			want: &model.AppMetadata{Links: []model.Link{
				{Label: "dashboard", URL: "https://dash"},
				{Label: "runbook", URL: "https://runbook"},
			}},
		},
		{
			name: "description and links",
			annotations: map[string]string{
				annoDescription:           "Billing API",
				annoLinkPrefix + "docs":   "https://docs",
				"unrelated":               "x",
				annoLinkPrefix + "":       "https://empty-label", // empty suffix skipped
				annoLinkPrefix + "broken": "",                    // empty value skipped
			},
			want: &model.AppMetadata{
				Description: "Billing API",
				Links:       []model.Link{{Label: "docs", URL: "https://docs"}},
			},
		},
		{
			name:        "nil map",
			annotations: nil,
			want:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := From(tt.annotations)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("From() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

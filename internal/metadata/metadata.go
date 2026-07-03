// Package metadata reads author-declared catalog metadata (description and
// reference links) from a workload's dfds.cloud/* annotations. It is a pure,
// network-free transform over the annotation map, mirroring the per-concern
// package layout used by gitops and swagger.
package metadata

import (
	"sort"
	"strings"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Author-declared metadata annotation keys.
const (
	annoDescription = "dfds.cloud/description" // free-form human description
	annoLinkPrefix  = "dfds.cloud/link."       // dfds.cloud/link.<label>=<url>
)

// From reads author-declared metadata from workload annotations. It returns nil
// when neither a description nor any dfds.cloud/link.* annotation is present, so
// the collector can leave ApplicationEntry.Metadata nil for the common case.
func From(annotations map[string]string) *model.AppMetadata {
	desc := annotations[annoDescription]

	var links []model.Link
	for k, v := range annotations {
		if v == "" || !strings.HasPrefix(k, annoLinkPrefix) {
			continue
		}
		label := strings.TrimPrefix(k, annoLinkPrefix)
		if label == "" {
			continue
		}
		links = append(links, model.Link{Label: label, URL: v})
	}
	// Deterministic ordering — map iteration is randomised.
	sort.Slice(links, func(i, j int) bool { return links[i].Label < links[j].Label })

	if desc == "" && len(links) == 0 {
		return nil
	}
	return &model.AppMetadata{Description: desc, Links: links}
}

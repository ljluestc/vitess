/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package messager

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// TestMessagezHandler verifies that /messagez lists every message table, sorted
// by name, with its configuration and its current number of receivers.
func TestMessagezHandler(t *testing.T) {
	engine := newTestEngine()
	t.Cleanup(engine.Close)
	engine.schemaChanged(nil, []*schema.Table{meTableT2, meTableT1}, nil, nil, true)

	f1, ch1 := newEngineReceiver()
	_, err := engine.Subscribe(t.Context(), "t1", f1)
	require.NoError(t, err)
	// The first response carries the fields, and it is sent once the receiver is registered.
	<-ch1

	resp := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/messagez", nil)
	engine.messagezHandler(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	body := resp.Body.String()

	row := func(name, receivers string) string {
		return strings.Join([]string{
			`<td>` + name + `</td>`,
			`<td>true</td>`,
			`<td>` + receivers + `</td>`,
			`<td>\d+</td>`,
			`<td>\d+</td>`,
			`<td>10</td>`,
			`<td>(true|false)</td>`,
			`<td>1s</td>`,
			`<td>3s</td>`,
			`<td>1</td>`,
			`<td>1s</td>`,
			`<td>1s</td>`,
			`<td>0s</td>`,
		}, `\s*`)
	}
	assert.Regexp(t, regexp.MustCompile(row("t1", "1")+`(?s:.*)`+row("t2", "0")), body)
}

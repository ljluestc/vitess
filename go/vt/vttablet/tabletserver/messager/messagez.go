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
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/safehtml/template"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logz"
)

var (
	messagezHeader = []byte(`
		<tr>
			<th>Table</th>
			<th>Open</th>
			<th>Receivers</th>
			<th>Queued</th>
			<th>In Flight</th>
			<th>Cache Size</th>
			<th>Messages Pending</th>
			<th>Ack Wait</th>
			<th>Purge After</th>
			<th>Batch Size</th>
			<th>Poll Interval</th>
			<th>Min Backoff</th>
			<th>Max Backoff</th>
		</tr>
	`)
	messagezTmpl = template.Must(template.New("messagez").Parse(`
		<tr class="low">
			<td>{{.Name}}</td>
			<td>{{.IsOpen}}</td>
			<td>{{.Receivers}}</td>
			<td>{{.Queued}}</td>
			<td>{{.InFlight}}</td>
			<td>{{.CacheSize}}</td>
			<td>{{.MessagesPending}}</td>
			<td>{{.AckWaitTime}}</td>
			<td>{{.PurgeAfter}}</td>
			<td>{{.BatchSize}}</td>
			<td>{{.PollInterval}}</td>
			<td>{{.MinBackoff}}</td>
			<td>{{.MaxBackoff}}</td>
		</tr>
	`))
)

// managerStatus is a point-in-time snapshot of a messageManager.
type managerStatus struct {
	Name            string
	IsOpen          bool
	Receivers       int
	Queued          int
	InFlight        int
	CacheSize       int
	MessagesPending bool
	AckWaitTime     time.Duration
	PurgeAfter      time.Duration
	BatchSize       int
	PollInterval    time.Duration
	MinBackoff      time.Duration
	MaxBackoff      time.Duration
}

// status returns a snapshot of the manager's configuration and its current state.
func (mm *messageManager) status() managerStatus {
	mm.mu.Lock()
	st := managerStatus{
		Name:            mm.name.String(),
		IsOpen:          mm.isOpen,
		Receivers:       len(mm.receivers),
		MessagesPending: mm.messagesPending,
		AckWaitTime:     mm.ackWaitTime,
		PurgeAfter:      mm.purgeAfter,
		BatchSize:       mm.batchSize,
		PollInterval:    mm.pollerTicks.Interval(),
		MinBackoff:      mm.minBackoff,
		MaxBackoff:      mm.maxBackoff,
	}
	mm.mu.Unlock()

	st.Queued, st.InFlight, st.CacheSize = mm.cache.stats()
	return st
}

// statuses returns a snapshot of every message manager, sorted by table name.
func (me *Engine) statuses() []managerStatus {
	me.managersMu.Lock()
	managers := make([]*messageManager, 0, len(me.managers))
	for _, mm := range me.managers {
		managers = append(managers, mm)
	}
	me.managersMu.Unlock()

	statuses := make([]managerStatus, 0, len(managers))
	for _, mm := range managers {
		statuses = append(statuses, mm.status())
	}
	slices.SortFunc(statuses, func(a, b managerStatus) int {
		return strings.Compare(a.Name, b.Name)
	})
	return statuses
}

// messagezHandler serves /messagez, which shows the configuration and the
// current state of every message table on this tablet.
func (me *Engine) messagezHandler(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(messagezHeader)

	for _, st := range me.statuses() {
		if err := messagezTmpl.Execute(w, st); err != nil {
			log.Error(fmt.Sprintf("messagez: couldn't execute template: %v", err))
		}
	}
}

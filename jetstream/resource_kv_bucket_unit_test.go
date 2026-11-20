// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

func TestMergeKVBucketStreamUpdate_PreservesUnmanagedAndAppliesManaged(t *testing.T) {
	live := jetstream.StreamConfig{
		Name:              "KV_TEST",
		Subjects:          []string{"$KV.TEST.>"},
		Description:       "live-desc",
		MaxMsgsPerSubject: 5,
		MaxBytes:          1000,
		MaxAge:            time.Minute,
		MaxMsgSize:        64,
		Replicas:          1,
		Placement:         &jetstream.Placement{Cluster: "old"},
		AllowDirect:       false,
		DenyDelete:        false,
		DenyPurge:         true,
		AllowRollup:       true,
		Discard:           jetstream.DiscardNew,
		Compression:       jetstream.S2Compression,
		RePublish:         &jetstream.RePublish{Source: "in.>", Destination: "out.>"},
		Metadata:          map[string]string{"env": "prod", "team": "platform"},
		Duplicates:        90 * time.Second,
		MaxMsgs:           -1,
		MaxConsumers:      -1,
		Storage:           jetstream.FileStorage,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		Description: "new-desc",
		MaxMsgSize:  128,
		History:     10,
		MaxAge:      2 * time.Minute,
		MaxBytes:    5000,
		Replicas:    3,
		Placement:   &jetstream.Placement{Cluster: "new", Tags: []string{"ssd"}},
		MarkerTTL:   45 * time.Second,
		// allow_direct / deny_delete unset → preserve live
	})

	if diff := cmp.Diff(false, got.AllowDirect); diff != "" {
		t.Errorf("AllowDirect preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(false, got.DenyDelete); diff != "" {
		t.Errorf("DenyDelete preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(true, got.DenyPurge); diff != "" {
		t.Errorf("DenyPurge preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(true, got.AllowRollup); diff != "" {
		t.Errorf("AllowRollup preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(jetstream.DiscardNew, got.Discard); diff != "" {
		t.Errorf("Discard preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(jetstream.S2Compression, got.Compression); diff != "" {
		t.Errorf("Compression preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(live.RePublish, got.RePublish); diff != "" {
		t.Errorf("RePublish preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(live.Metadata, got.Metadata); diff != "" {
		t.Errorf("Metadata preserved mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(live.Duplicates, got.Duplicates); diff != "" {
		t.Errorf("Duplicates preserved mismatch (-want +got):\n%s", diff)
	}

	if got.Description != "new-desc" {
		t.Errorf("Description: want new-desc, got %q", got.Description)
	}
	if got.MaxMsgSize != 128 {
		t.Errorf("MaxMsgSize: want 128, got %d", got.MaxMsgSize)
	}
	if got.MaxMsgsPerSubject != 10 {
		t.Errorf("MaxMsgsPerSubject/history: want 10, got %d", got.MaxMsgsPerSubject)
	}
	if got.MaxAge != 2*time.Minute {
		t.Errorf("MaxAge: want 2m, got %s", got.MaxAge)
	}
	if got.MaxBytes != 5000 {
		t.Errorf("MaxBytes: want 5000, got %d", got.MaxBytes)
	}
	if got.Replicas != 3 {
		t.Errorf("Replicas: want 3, got %d", got.Replicas)
	}
	if got.Placement == nil || got.Placement.Cluster != "new" || len(got.Placement.Tags) != 1 || got.Placement.Tags[0] != "ssd" {
		t.Errorf("Placement: want cluster=new tags=[ssd], got %#v", got.Placement)
	}
	if got.SubjectDeleteMarkerTTL != 45*time.Second {
		t.Errorf("SubjectDeleteMarkerTTL: want 45s, got %s", got.SubjectDeleteMarkerTTL)
	}
	if !got.AllowMsgTTL {
		t.Errorf("AllowMsgTTL: want true when MarkerTTL > 0")
	}
}

func TestMergeKVBucketStreamUpdate_ExplicitFlagOverrides(t *testing.T) {
	live := jetstream.StreamConfig{
		Name:        "KV_TEST",
		AllowDirect: false,
		DenyDelete:  false,
		DenyPurge:   true,
		AllowRollup: true,
		Discard:     jetstream.DiscardOld,
		Compression: jetstream.NoCompression,
		Replicas:    1,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		Description:    "x",
		History:        5,
		Replicas:       1,
		AllowDirect:    true,
		AllowDirectSet: true,
		DenyDelete:     true,
		DenyDeleteSet:  true,
	})

	if !got.AllowDirect {
		t.Errorf("AllowDirect: want true override, got false")
	}
	if !got.DenyDelete {
		t.Errorf("DenyDelete: want true override, got false")
	}
	// Unmanaged flags still preserved
	if !got.DenyPurge {
		t.Errorf("DenyPurge: want preserved true")
	}
	if !got.AllowRollup {
		t.Errorf("AllowRollup: want preserved true")
	}
	if got.Discard != jetstream.DiscardOld {
		t.Errorf("Discard: want preserved DiscardOld, got %v", got.Discard)
	}
	if got.Compression != jetstream.NoCompression {
		t.Errorf("Compression: want preserved NoCompression, got %v", got.Compression)
	}
}

func TestMergeKVBucketStreamUpdate_UnsetFlagsDoNotClobberLiveTrue(t *testing.T) {
	live := jetstream.StreamConfig{
		AllowDirect: true,
		DenyDelete:  true,
		DenyPurge:   false,
		AllowRollup: false,
		Replicas:    2,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		Replicas: 3,
		History:  8,
		// flags unset
	})

	if !got.AllowDirect || !got.DenyDelete {
		t.Errorf("expected live allow_direct/deny_delete true preserved, got allow_direct=%v deny_delete=%v", got.AllowDirect, got.DenyDelete)
	}
	if got.DenyPurge || got.AllowRollup {
		t.Errorf("expected live deny_purge/allow_rollup false preserved, got deny_purge=%v allow_rollup=%v", got.DenyPurge, got.AllowRollup)
	}
	if got.Replicas != 3 {
		t.Errorf("Replicas: want 3, got %d", got.Replicas)
	}
}

func TestMergeKVBucketStreamUpdate_DiscardSetOld(t *testing.T) {
	live := jetstream.StreamConfig{
		Name:     "KV_TEST",
		Discard:  jetstream.DiscardNew,
		Replicas: 1,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		History:    5,
		Replicas:   1,
		Discard:    jetstream.DiscardOld,
		DiscardSet: true,
	})

	if got.Discard != jetstream.DiscardOld {
		t.Errorf("Discard: want DiscardOld override, got %v", got.Discard)
	}
}

func TestMergeKVBucketStreamUpdate_DiscardSetNew(t *testing.T) {
	live := jetstream.StreamConfig{
		Name:     "KV_TEST",
		Discard:  jetstream.DiscardOld,
		Replicas: 1,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		History:    5,
		Replicas:   1,
		Discard:    jetstream.DiscardNew,
		DiscardSet: true,
	})

	if got.Discard != jetstream.DiscardNew {
		t.Errorf("Discard: want DiscardNew override, got %v", got.Discard)
	}
}

func TestMergeKVBucketStreamUpdate_DiscardUnsetPreservesLive(t *testing.T) {
	live := jetstream.StreamConfig{
		Name:     "KV_TEST",
		Discard:  jetstream.DiscardOld,
		Replicas: 1,
	}

	got := mergeKVBucketStreamUpdate(live, kvBucketStreamUpdate{
		History:  5,
		Replicas: 1,
		// DiscardSet unset → preserve live
	})

	if got.Discard != jetstream.DiscardOld {
		t.Errorf("Discard: want preserved DiscardOld, got %v", got.Discard)
	}
}

func TestValidateKVBucketDenyDeleteChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		old     bool
		new     bool
		newSet  bool
		wantErr string
	}{
		{name: "reject true to false", old: true, new: false, newSet: true, wantErr: "deny_delete cannot be changed from true to false"},
		{name: "allow false to true", old: false, new: true, newSet: true},
		{name: "allow unchanged true", old: true, new: true, newSet: true},
		{name: "allow unchanged false", old: false, new: false, newSet: true},
		{name: "allow unset when live true", old: true, new: false, newSet: false},
		{name: "allow unset when live false", old: false, new: false, newSet: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateKVBucketDenyDeleteChange(tt.old, tt.new, tt.newSet)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

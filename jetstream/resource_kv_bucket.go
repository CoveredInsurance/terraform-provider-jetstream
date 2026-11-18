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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func resourceKVBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKVBucketCreate,
		Read:   resourceKVBucketRead,
		Update: resourceKVBucketUpdate,
		Delete: resourceKVBucketDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: resourceKVBucketCustomizeDiff,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Description: "The name of the Bucket",
				Required:    true,
				ForceNew:    true,
			},
			"description": {
				Type:        schema.TypeString,
				Description: "Contains additional information about this bucket",
				Optional:    true,
				ForceNew:    false,
			},
			"storage": {
				Type:             schema.TypeString,
				Description:      "The storage engine to use to back the bucket",
				Default:          "file",
				ForceNew:         true,
				Optional:         true,
				ValidateDiagFunc: validateStorageTypeString(),
			},
			"history": {
				Type:         schema.TypeInt,
				Description:  "How many historical values to keep",
				Default:      5,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.All(validation.IntAtLeast(0), validation.IntAtMost(128)),
			},
			"ttl": {
				Type:         schema.TypeInt,
				Description:  "How many seconds a value will be kept in the bucket",
				Optional:     true,
				ForceNew:     false,
				Default:      0,
				ValidateFunc: validation.IntAtLeast(0),
			},
			"max_value_size": {
				Type:         schema.TypeInt,
				Description:  "Maximum size of any value",
				Default:      -1,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"max_bucket_size": {
				Type:         schema.TypeInt,
				Description:  "Maximum size of the entire bucket",
				Default:      -1,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"placement_cluster": {
				Type:        schema.TypeString,
				Description: "Place the bucket in a specific cluster, influenced by placement_tags",
				Default:     "",
				Optional:    true,
			},
			"placement_tags": {
				Type:        schema.TypeList,
				Description: "Place the stream only on servers with these tags",
				Optional:    true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"replicas": {
				Type:         schema.TypeInt,
				Description:  "Number of cluster replicas to store",
				Default:      1,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.All(validation.IntAtLeast(1), validation.IntAtMost(5)),
			},
			"limit_marker_ttl": {
				Type:         schema.TypeInt,
				Description:  "Enables Per-Key TTLs and Limit Markers, duration specified in seconds",
				Optional:     true,
				ForceNew:     false,
				Default:      0,
				ValidateFunc: validation.IntAtLeast(0),
			},
			"allow_direct": {
				Type:        schema.TypeBool,
				Description: "Allow direct get on the backing stream. When unset, the live stream value is preserved.",
				Optional:    true,
				Computed:    true,
			},
			"deny_delete": {
				Type:        schema.TypeBool,
				Description: "Restrict deleting messages from the backing stream via the API. Cannot be changed from true to false. When unset, the live stream value is preserved.",
				Optional:    true,
				Computed:    true,
			},
		},
	}
}

func resourceKVBucketCustomizeDiff(ctx context.Context, d *schema.ResourceDiff, meta any) error {
	raw := d.GetRawConfig()
	if raw.IsNull() {
		return nil
	}
	v := raw.GetAttr("deny_delete")
	if v.IsNull() || !v.IsKnown() {
		return validateKVBucketDenyDeleteChange(false, false, false)
	}
	old, _ := d.GetChange("deny_delete")
	oldVal, _ := old.(bool)
	return validateKVBucketDenyDeleteChange(oldVal, v.True(), true)
}

// validateKVBucketDenyDeleteChange rejects disabling deny_delete once enabled.
// When newSet is false the attribute is unset in config and the live value is kept.
func validateKVBucketDenyDeleteChange(oldDenyDelete, newDenyDelete, newSet bool) error {
	if !newSet {
		return nil
	}
	if oldDenyDelete && !newDenyDelete {
		return fmt.Errorf("deny_delete cannot be changed from true to false: the NATS server forbids disabling deny_delete once enabled")
	}
	return nil
}

// kvBucketStreamUpdate holds TF-managed fields applied onto a live stream config.
type kvBucketStreamUpdate struct {
	Description    string
	MaxMsgSize     int32
	History        int64
	MaxAge         time.Duration
	MaxBytes       int64
	Replicas       int
	Placement      *jetstream.Placement
	MarkerTTL      time.Duration
	AllowDirect    bool
	AllowDirectSet bool
	DenyDelete     bool
	DenyDeleteSet  bool
}

// mergeKVBucketStreamUpdate copies live, applies TF-managed fields, and only
// overrides allow_direct / deny_delete when those attributes are set in config.
func mergeKVBucketStreamUpdate(live jetstream.StreamConfig, u kvBucketStreamUpdate) jetstream.StreamConfig {
	cfg := live
	cfg.Description = u.Description
	cfg.MaxMsgSize = u.MaxMsgSize
	cfg.MaxMsgsPerSubject = u.History
	cfg.MaxAge = u.MaxAge
	cfg.MaxBytes = u.MaxBytes
	// Apply configured replicas (v0.4.0 copied live Replicas and never
	// updated; upstream fix df44c21). Required so 1→3 reaches the server.
	cfg.Replicas = u.Replicas
	cfg.Placement = u.Placement
	cfg.SubjectDeleteMarkerTTL = u.MarkerTTL
	if u.MarkerTTL > 0 {
		cfg.AllowMsgTTL = true
	}
	if u.AllowDirectSet {
		cfg.AllowDirect = u.AllowDirect
	}
	if u.DenyDeleteSet {
		cfg.DenyDelete = u.DenyDelete
	}
	return cfg
}

// kvBucketConfigBool returns the configured value for an Optional+Computed bool
// and whether it was set in Terraform configuration (not merely present in state).
func kvBucketConfigBool(d *schema.ResourceData, key string) (bool, bool) {
	raw := d.GetRawConfig()
	if raw.IsNull() {
		return false, false
	}
	v := raw.GetAttr(key)
	if v.IsNull() || !v.IsKnown() {
		return false, false
	}
	return v.True(), true
}

func kvBucketPlacement(d *schema.ResourceData) *jetstream.Placement {
	c, ok := d.GetOk("placement_cluster")
	if !ok {
		return nil
	}
	placement := &jetstream.Placement{Cluster: c.(string)}
	pt, ok := d.GetOk("placement_tags")
	if ok {
		ts := pt.([]any)
		tags := make([]string, len(ts))
		for i, tag := range ts {
			tags[i] = tag.(string)
		}
		placement.Tags = tags
	}
	return placement
}

func kvBucketStorage(d *schema.ResourceData) jetstream.StorageType {
	switch d.Get("storage").(string) {
	case "memory":
		return jetstream.MemoryStorage
	default:
		return jetstream.FileStorage
	}
}

// kvBucketStreamConfig builds a JetStream stream config matching nats.go KV
// defaults (ADR-8), then applies explicitly configured allow_direct / deny_delete.
func kvBucketStreamConfig(d *schema.ResourceData) jetstream.StreamConfig {
	name := d.Get("name").(string)
	history := d.Get("history").(int)
	ttl := time.Duration(d.Get("ttl").(int)) * time.Second
	maxV := d.Get("max_value_size").(int)
	maxB := d.Get("max_bucket_size").(int)
	replicas := d.Get("replicas").(int)
	description := d.Get("description").(string)
	markerTTL := time.Duration(d.Get("limit_marker_ttl").(int)) * time.Second

	duplicateWindow := 2 * time.Minute
	if ttl > 0 && ttl < duplicateWindow {
		duplicateWindow = ttl
	}

	scfg := jetstream.StreamConfig{
		Name:                   fmt.Sprintf("KV_%s", name),
		Subjects:               []string{fmt.Sprintf("$KV.%s.>", name)},
		Description:            description,
		MaxMsgsPerSubject:      int64(history),
		MaxBytes:               int64(maxB),
		MaxAge:                 ttl,
		MaxMsgSize:             int32(maxV),
		Storage:                kvBucketStorage(d),
		Replicas:               replicas,
		Placement:              kvBucketPlacement(d),
		AllowRollup:            true,
		DenyDelete:             true,
		Duplicates:             duplicateWindow,
		MaxMsgs:                -1,
		MaxConsumers:           -1,
		AllowDirect:            true,
		Discard:                jetstream.DiscardNew,
		AllowMsgTTL:            markerTTL > 0,
		SubjectDeleteMarkerTTL: markerTTL,
	}

	if v, set := kvBucketConfigBool(d, "allow_direct"); set {
		scfg.AllowDirect = v
	}
	if v, set := kvBucketConfigBool(d, "deny_delete"); set {
		scfg.DenyDelete = v
	}

	return scfg
}

func applyKVBucketStreamFlagOverrides(ctx context.Context, js jetstream.JetStream, name string, d *schema.ResourceData) error {
	allowDirect, allowDirectSet := kvBucketConfigBool(d, "allow_direct")
	denyDelete, denyDeleteSet := kvBucketConfigBool(d, "deny_delete")
	if !allowDirectSet && !denyDeleteSet {
		return nil
	}

	str, err := js.Stream(ctx, fmt.Sprintf("KV_%s", name))
	if err != nil {
		return err
	}
	info, err := str.Info(ctx)
	if err != nil {
		return err
	}
	cfg := info.Config
	changed := false
	if allowDirectSet && cfg.AllowDirect != allowDirect {
		cfg.AllowDirect = allowDirect
		changed = true
	}
	if denyDeleteSet && cfg.DenyDelete != denyDelete {
		cfg.DenyDelete = denyDelete
		changed = true
	}
	if !changed {
		return nil
	}
	_, err = js.UpdateStream(ctx, cfg)
	return err
}

func resourceKVBucketCreate(d *schema.ResourceData, m any) error {
	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	name := d.Get("name").(string)
	history := d.Get("history").(int)
	ttl := d.Get("ttl").(int)
	maxV := d.Get("max_value_size").(int)
	maxB := d.Get("max_bucket_size").(int)
	replicas := d.Get("replicas").(int)
	descrption := d.Get("description").(string)
	limit_marker_ttl := d.Get("limit_marker_ttl").(int)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	known, err := js.KeyValue(ctx, name)
	if known != nil {
		return fmt.Errorf("bucket %s already exist", name)
	} else if err != nil {
		if !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("failed to load KV bucket: %s", err)
		}
	}

	denyDelete, denyDeleteSet := kvBucketConfigBool(d, "deny_delete")
	// nats.go CreateKeyValue always sets DenyDelete=true; the server forbids
	// flipping that to false afterward, so create the backing stream directly.
	if denyDeleteSet && !denyDelete {
		_, err = js.CreateStream(ctx, kvBucketStreamConfig(d))
		if err != nil {
			return err
		}
	} else {
		_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:         name,
			Description:    descrption,
			MaxValueSize:   int32(maxV),
			History:        uint8(history),
			TTL:            time.Duration(ttl) * time.Second,
			MaxBytes:       int64(maxB),
			Storage:        kvBucketStorage(d),
			Replicas:       replicas,
			Placement:      kvBucketPlacement(d),
			LimitMarkerTTL: time.Duration(limit_marker_ttl) * time.Second,
		})
		if err != nil {
			return err
		}
		if err := applyKVBucketStreamFlagOverrides(ctx, js, name, d); err != nil {
			return err
		}
	}

	d.SetId(fmt.Sprintf("JETSTREAM_KV_%s", name))

	return resourceKVBucketRead(d, m)
}

func resourceKVBucketRead(d *schema.ResourceData, m any) error {
	name, err := parseStreamKVID(d.Id())
	if err != nil {
		return err
	}

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	bucket, err := js.KeyValue(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			d.SetId("")
			return nil
		}
		return err
	}
	status, err := bucket.Status(ctx)
	if err != nil {
		return err
	}

	d.Set("name", status.Bucket())
	d.Set("history", status.History())
	d.Set("ttl", status.TTL().Seconds())

	jStatus := status.(*jetstream.KeyValueBucketStatus)
	si := jStatus.StreamInfo()

	switch si.Config.Storage {
	case jetstream.FileStorage:
		d.Set("storage", "file")
	case jetstream.MemoryStorage:
		d.Set("storage", "memory")
	}

	d.Set("max_value_size", si.Config.MaxMsgSize)
	d.Set("max_bucket_size", si.Config.MaxBytes)
	d.Set("replicas", si.Config.Replicas)
	d.Set("description", si.Config.Description)
	d.Set("allow_direct", si.Config.AllowDirect)
	d.Set("deny_delete", si.Config.DenyDelete)

	if si.Config.Placement != nil {
		d.Set("placement_cluster", si.Config.Placement.Cluster)
		d.Set("placement_tags", si.Config.Placement.Tags)
	}

	d.Set("limit_marker_ttl", si.Config.SubjectDeleteMarkerTTL.Seconds())

	return nil
}

func resourceKVBucketUpdate(d *schema.ResourceData, m any) error {
	name := d.Get("name").(string)

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	bucket, err := js.KeyValue(ctx, name)
	if err != nil {
		return err
	}
	status, err := bucket.Status(ctx)
	if err != nil {
		return err
	}

	jStatus := status.(*jetstream.KeyValueBucketStatus)

	str, err := js.Stream(ctx, jStatus.StreamInfo().Config.Name)
	if err != nil {
		return err
	}

	info, err := str.Info(ctx)
	if err != nil {
		return err
	}

	allowDirect, allowDirectSet := kvBucketConfigBool(d, "allow_direct")
	denyDelete, denyDeleteSet := kvBucketConfigBool(d, "deny_delete")
	cfg := mergeKVBucketStreamUpdate(info.Config, kvBucketStreamUpdate{
		Description:    d.Get("description").(string),
		MaxMsgSize:     int32(d.Get("max_value_size").(int)),
		History:        int64(d.Get("history").(int)),
		MaxAge:         time.Duration(d.Get("ttl").(int)) * time.Second,
		MaxBytes:       int64(d.Get("max_bucket_size").(int)),
		Replicas:       d.Get("replicas").(int),
		Placement:      kvBucketPlacement(d),
		MarkerTTL:      time.Duration(d.Get("limit_marker_ttl").(int)) * time.Second,
		AllowDirect:    allowDirect,
		AllowDirectSet: allowDirectSet,
		DenyDelete:     denyDelete,
		DenyDeleteSet:  denyDeleteSet,
	})

	_, err = js.UpdateStream(ctx, cfg)
	if err != nil {
		return err
	}

	return resourceKVBucketRead(d, m)
}

func resourceKVBucketDelete(d *schema.ResourceData, m any) error {
	name := d.Get("name").(string)

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = js.DeleteKeyValue(ctx, name)
	if err == nats.ErrStreamNotFound {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

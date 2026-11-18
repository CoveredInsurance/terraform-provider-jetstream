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
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testKV_basic = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_kv_bucket" "test" {
  name = "TEST"
  ttl = 60
  storage = "memory"
  history = 10
  max_value_size = 1024
  max_bucket_size = 10240
  limit_marker_ttl = 45
}
`

const testKV_preserveFlags = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_kv_bucket" "test" {
  name         = "TEST"
  storage      = "memory"
  history      = 10
  replicas     = %d
  allow_direct = false
  deny_delete  = false
}
`

const testKV_allowDirect = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_kv_bucket" "test" {
  name         = "TEST"
  storage      = "memory"
  history      = 5
  allow_direct = %t
}
`

const testKV_denyDelete = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_kv_bucket" "test" {
  name        = "TEST"
  storage     = "memory"
  history     = 5
  deny_delete = %t
}
`

func TestResourceKV(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "name", "TEST"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "ttl", "60"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "storage", "memory"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "history", "10"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "max_value_size", "1024"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "max_bucket_size", "10240"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "limit_marker_ttl", "45"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "allow_direct", "true"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "deny_delete", "true"),
				),
			},
			{
				ResourceName:      "jetstream_kv_bucket.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestResourceKVPreserveStreamFlagsOnUpdate(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_preserveFlags, nc.ConnectedUrl(), 1),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamFlags(t, nc, "TEST", false, false),
					testBucketStreamReplicas(t, nc, "TEST", 1),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "allow_direct", "false"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "deny_delete", "false"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "replicas", "1"),
				),
			},
			{
				// replicas 1→3 must reach the server (v0.4.0 bug / upstream df44c21)
				// and must not flip allow_direct / deny_delete.
				Config: fmt.Sprintf(testKV_preserveFlags, nc.ConnectedUrl(), 3),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamFlags(t, nc, "TEST", false, false),
					testBucketStreamReplicas(t, nc, "TEST", 3),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "allow_direct", "false"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "deny_delete", "false"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "replicas", "3"),
				),
			},
		},
	})
}

const testKV_replicas = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_kv_bucket" "test" {
  name     = "TEST"
  storage  = "memory"
  history  = 5
  replicas = %d
}
`

func TestResourceKVReplicasUpdate(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	// Single-node JetStream accepts num_replicas updates into stream config
	// (verified against nats-server 2.11+/2.14); assert live StreamInfo, not
	// only Terraform state, so a recurring plan bug cannot hide here.
	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_replicas, nc.ConnectedUrl(), 1),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamReplicas(t, nc, "TEST", 1),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "replicas", "1"),
				),
			},
			{
				Config: fmt.Sprintf(testKV_replicas, nc.ConnectedUrl(), 3),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamReplicas(t, nc, "TEST", 3),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "replicas", "3"),
				),
			},
		},
	})
}

func TestResourceKVAllowDirectUpdate(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_allowDirect, nc.ConnectedUrl(), true),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamFlags(t, nc, "TEST", true, true),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "allow_direct", "true"),
				),
			},
			{
				Config: fmt.Sprintf(testKV_allowDirect, nc.ConnectedUrl(), false),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					testBucketStreamFlags(t, nc, "TEST", false, true),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "allow_direct", "false"),
				),
			},
		},
	})
}

func TestResourceKVDenyDeleteTrueToFalseRejected(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_denyDelete, nc.ConnectedUrl(), true),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "deny_delete", "true"),
				),
			},
			{
				Config:      fmt.Sprintf(testKV_denyDelete, nc.ConnectedUrl(), false),
				ExpectError: regexp.MustCompile(`deny_delete cannot be changed from true to false`),
			},
		},
	})
}

func TestResourceKVExternalDeletion(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testKV_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "name", "TEST"),
				),
			},
			{
				Config: fmt.Sprintf(testKV_basic, nc.ConnectedUrl()),
				PreConfig: func() {
					err := js.DeleteKeyValue(ctx, "TEST")
					if err != nil {
						t.Fatalf("failed to externally delete bucket: %s", err)
					}
				},
				Check: resource.ComposeTestCheckFunc(
					testBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_kv_bucket.test", "name", "TEST"),
				),
			},
		},
	})
}

func testBucketDoesNotExist(t *testing.T, mgr *jsm.Manager, bucket string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		err := testBucketExist(t, mgr, bucket)
		if err == nil {
			return fmt.Errorf("expected bucket %q to not exist", bucket)
		}

		return nil
	}
}

func testBucketExist(t *testing.T, mgr *jsm.Manager, bucket string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		known, err := mgr.IsKnownStream("KV_" + bucket)
		if err != nil {
			return err
		}

		if !known {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}

		return nil
	}
}

func testBucketStreamFlags(t *testing.T, nc *nats.Conn, bucket string, allowDirect, denyDelete bool) resource.TestCheckFunc {
	t.Helper()
	return func(s *terraform.State) error {
		info, err := kvBucketStreamInfo(nc, bucket)
		if err != nil {
			return err
		}
		if info.Config.AllowDirect != allowDirect {
			return fmt.Errorf("allow_direct: want %v, got %v", allowDirect, info.Config.AllowDirect)
		}
		if info.Config.DenyDelete != denyDelete {
			return fmt.Errorf("deny_delete: want %v, got %v", denyDelete, info.Config.DenyDelete)
		}
		return nil
	}
}

func testBucketStreamReplicas(t *testing.T, nc *nats.Conn, bucket string, replicas int) resource.TestCheckFunc {
	t.Helper()
	return func(s *terraform.State) error {
		info, err := kvBucketStreamInfo(nc, bucket)
		if err != nil {
			return err
		}
		if info.Config.Replicas != replicas {
			return fmt.Errorf("replicas: want %d, got %d", replicas, info.Config.Replicas)
		}
		return nil
	}
}

func kvBucketStreamInfo(nc *nats.Conn, bucket string) (*jetstream.StreamInfo, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	str, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return nil, err
	}
	return str.Info(ctx)
}

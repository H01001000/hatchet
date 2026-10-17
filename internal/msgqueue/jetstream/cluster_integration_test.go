//go:build integration

package jetstream

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

func TestMessageQueueReplicatedLeaderAndQuorumLoss(t *testing.T) {
	urls := os.Getenv("HATCHET_TEST_JETSTREAM_CLUSTER_URLS")
	if urls == "" {
		t.Skip("set HATCHET_TEST_JETSTREAM_CLUSTER_URLS for the dedicated three-broker test cluster")
	}
	t.Setenv("HATCHET_TEST_JETSTREAM_URL", urls)
	f := newTestMessageQueue(t, WithReplicas(3), WithAckWait(5*time.Second))
	queue := msgqueue.NewRandomStaticQueue()
	for i := range 10 {
		require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, i)))
	}
	name, _ := f.q.queueNames(queue)
	stream, err := f.admin.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.NotNil(t, info.Cluster)
	leader := info.Cluster.Leader
	require.Contains(t, []string{"js-a", "js-b", "js-c"}, leader)
	container := "hatchet-jetstream-cluster-" + strings.TrimPrefix(leader, "js-")
	restart := func(name string) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", "start", name).CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	t.Cleanup(func() { restart(container) })
	output, err := exec.CommandContext(t.Context(), "docker", "stop", container).CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Eventually(t, func() bool {
		info, err := stream.Info(t.Context())
		return err == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != leader
	}, 20*time.Second, 100*time.Millisecond)
	received := make(chan int, 32)
	var cleanup func() error
	require.Eventually(t, func() bool {
		var err error
		cleanup, err = f.q.Subscribe(queue, func(msg *msgqueue.Message) error {
			var id int
			if err := json.Unmarshal(msg.Payloads[0], &id); err != nil {
				return err
			}
			received <- id
			return nil
		}, msgqueue.NoOpHook)
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)
	t.Cleanup(func() { require.NoError(t, cleanup()) })
	seen := map[int]bool{}
	for len(seen) < 10 {
		seen[receive(t, received)] = true
	}
	for i := range 10 {
		require.True(t, seen[i], "acknowledged publication %d was lost", i)
	}
	waitDrained(t, f, queue)
	second := "hatchet-jetstream-cluster-a"
	if second == container {
		second = "hatchet-jetstream-cluster-b"
	}
	t.Cleanup(func() { restart(second) })
	output, err = exec.CommandContext(t.Context(), "docker", "stop", second).CombinedOutput()
	require.NoError(t, err, "%s", output)
	ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
	err = f.q.SendMessage(ctx, queue, fixtureMessage(t, 20))
	cancel()
	require.Error(t, err, "a single remaining replica must not acknowledge a new publication")
	restart(second)
	require.Eventually(t, func() bool { return f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 21)) == nil }, 20*time.Second, 100*time.Millisecond)
}

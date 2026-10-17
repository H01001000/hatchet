package loader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/config/server"
)

func TestJetStreamConfigDefaultsAndPubSubInheritance(t *testing.T) {
	t.Setenv("SERVER_MSGQUEUE_KIND", "jetstream")
	t.Setenv("SERVER_MSGQUEUE_JETSTREAM_URL", "nats://broker:4222")
	t.Setenv("SERVER_MSGQUEUE_JETSTREAM_USERNAME", "worker")
	t.Setenv("SERVER_MSGQUEUE_JETSTREAM_PASSWORD", "test-password")
	t.Setenv("SERVER_MSGQUEUE_JETSTREAM_TLS_ENABLED", "true")
	t.Setenv("SERVER_MSGQUEUE_JETSTREAM_TLS_ROOT_CA_FILE", "/test/ca.pem")
	cf, err := LoadServerConfigFile()
	require.NoError(t, err)
	cfg := cf.MessageQueue.JetStream
	require.Equal(t, "nats://broker:4222", cfg.URL)
	require.Equal(t, 3, cfg.Replicas)
	require.Equal(t, 100, cfg.Qos)
	require.EqualValues(t, 1<<30, cfg.MaxBytes)
	require.Equal(t, "HATCHET_MQ", cfg.StreamPrefix)
	require.Equal(t, 5*time.Minute, cfg.AckWait)
	require.Equal(t, 20*time.Second, cfg.DispatcherTTL)
	require.Equal(t, 10*time.Minute, cfg.InactivityTTL)
	require.Equal(t, 15*time.Second, cfg.LeaseTimeout)
	kind, _ := resolvePubSubKindAndURL(cf)
	require.Equal(t, "nats", kind)
	nats := resolveNATSPubSubConfig(cf)
	require.Equal(t, cfg.URL, nats.URL)
	require.Equal(t, cfg.Username, nats.Username)
	require.Equal(t, cfg.Password, nats.Password)
	require.True(t, nats.TLSEnabled)
	require.Equal(t, cfg.TLSRootCAFile, nats.TLSRootCAFile)
	require.Empty(t, cf.MessageQueue.PubSub.NATS.URL, "resolving configuration must not mutate shared config")
}

func TestJetStreamConfigEnvironmentOverrides(t *testing.T) {
	for key, value := range map[string]string{
		"SERVER_MSGQUEUE_KIND": "jetstream", "SERVER_MSGQUEUE_JETSTREAM_URL": "nats://broker:4222",
		"SERVER_MSGQUEUE_JETSTREAM_STREAM_PREFIX": "ISOLATED", "SERVER_MSGQUEUE_JETSTREAM_REPLICAS": "1",
		"SERVER_MSGQUEUE_JETSTREAM_MAX_BYTES": "67108864", "SERVER_MSGQUEUE_JETSTREAM_QOS": "7",
		"SERVER_MSGQUEUE_JETSTREAM_COMPRESSION_ENABLED": "true", "SERVER_MSGQUEUE_JETSTREAM_COMPRESSION_THRESHOLD": "2048",
		"SERVER_MSGQUEUE_JETSTREAM_ENABLE_MESSAGE_REJECTION": "true", "SERVER_MSGQUEUE_JETSTREAM_MAX_DEATH_COUNT": "9",
		"SERVER_MSGQUEUE_JETSTREAM_ACK_WAIT": "10s", "SERVER_MSGQUEUE_JETSTREAM_REQUEST_TIMEOUT": "2s",
		"SERVER_MSGQUEUE_JETSTREAM_DEAD_LETTER_BACKOFF": "250ms", "SERVER_MSGQUEUE_JETSTREAM_DISPATCHER_TTL": "3s",
		"SERVER_MSGQUEUE_JETSTREAM_INACTIVITY_TTL": "1m", "SERVER_MSGQUEUE_JETSTREAM_LEASE_TIMEOUT": "6s",
		"SERVER_MSGQUEUE_JETSTREAM_SWEEP_INTERVAL": "100ms",
	} {
		t.Setenv(key, value)
	}
	cf, err := LoadServerConfigFile()
	require.NoError(t, err)
	cfg := cf.MessageQueue.JetStream
	require.Equal(t, "ISOLATED", cfg.StreamPrefix)
	require.Equal(t, 1, cfg.Replicas)
	require.EqualValues(t, 64<<20, cfg.MaxBytes)
	require.Equal(t, 7, cfg.Qos)
	require.True(t, cfg.CompressionEnabled)
	require.Equal(t, 2048, cfg.CompressionThreshold)
	require.True(t, cfg.EnableMessageRejection)
	require.Equal(t, 9, cfg.MaxDeathCount)
	require.Equal(t, 10*time.Second, cfg.AckWait)
	require.Equal(t, 2*time.Second, cfg.RequestTimeout)
	require.Equal(t, 250*time.Millisecond, cfg.DeadLetterBackoff)
	require.Equal(t, 3*time.Second, cfg.DispatcherTTL)
	require.Equal(t, time.Minute, cfg.InactivityTTL)
	require.Equal(t, 6*time.Second, cfg.LeaseTimeout)
	require.Equal(t, 100*time.Millisecond, cfg.SweepInterval)
}

func TestJetStreamExplicitPubSubOverrideDoesNotLeakQueueCredentials(t *testing.T) {
	cf := &server.ServerConfigFile{MessageQueue: server.MessageQueueConfigFile{
		Kind: "jetstream", JetStream: server.JetStreamConfigFile{URL: "nats://private:4222", Username: "private-user", Password: "private-password", TLSEnabled: true, TLSRootCAFile: "private.pem"},
		PubSub: server.PubSubConfigFile{Kind: "nats", NATS: server.PubSubNATSConfigFile{URL: "nats://other:4222"}},
	}}
	nats := resolveNATSPubSubConfig(cf)
	require.Equal(t, "nats://other:4222", nats.URL)
	require.Empty(t, nats.Username)
	require.Empty(t, nats.Password)
	require.False(t, nats.TLSEnabled)
	require.Empty(t, nats.TLSRootCAFile)
	cf.MessageQueue.PubSub.Kind = "postgres"
	kind, _ := resolvePubSubKindAndURL(cf)
	require.Equal(t, "postgres", kind)
}

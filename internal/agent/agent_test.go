package agent_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	api "github.com/nireo/dilog/api/v1"
	"github.com/nireo/dilog/internal/agent"
	"github.com/nireo/dilog/internal/config"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s", rpcAddr), opts...)
	if err != nil {
		t.Fatal(err)
	}

	client := api.NewLogClient(conn)

	return client
}

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.01",
	})

	if err != nil {
		t.Fatal(err)
	}

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		t.Fatal(err)
	}

	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.01", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		if err != nil {
			t.Fatal(err)
		}

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		if err != nil {
			t.Fatal(err)
		}

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			if err != nil {
				t.Fatal(err)
			}

			if err := os.RemoveAll(agent.Config.DataDir); err != nil {
				t.Fatal(err)
			}
		}
	}()

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	consumeResponse, err := leaderClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(consumeResponse.Record.Value, []byte("foo")) {
		t.Fatal("values are not equal")
	}

	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(consumeResponse.Record.Value, []byte("foo")) {
		t.Fatal("values are not equal")
	}
}

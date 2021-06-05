package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/nireo/dilog/api/v1"
	"github.com/nireo/dilog/internal/log"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTests(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTests(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	if err != nil {
		t.Fatal(err)
	}

	dir, err := ioutil.TempDir("", "server-test")
	if err != nil {
		t.Fatal(err)
	}

	clog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatal(err)
	}

	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	if err != nil {
		t.Error(err)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})

	if err != nil {
		t.Error(err)
	}

	if want.Offset != consume.Record.Offset {
		t.Error("offsets don't match")
	}

	if !bytes.Equal(want.Value, consume.Record.Value) {
		t.Error("offsets don't match")
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if err != nil {
		t.Error(err)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		if err != nil {
			t.Error(err)
		}

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				t.Error(err)
			}

			res, err := stream.Recv()
			if err != nil {
				t.Error(err)
			}

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		if err != nil {
			t.Fatal(err)
		}

		for i, record := range records {
			res, err := stream.Recv()
			if err != nil {
				t.Error(err)
			}

			if !bytes.Equal(res.Record.Value, record.Value) {
				t.Fatal("values don't match")
			}

			if res.Record.Offset != uint64(i) {
				t.Fatal("offsets don't match")
			}
		}
	}
}

package etcd

import (
	"errors"
	"os"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/log"
	"golang.org/x/net/context"
)

func TestPublisher(t *testing.T) {
	var (
		logger = log.NewNopLogger()
		e      = func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil }
	)

	factory := func(instance string) (endpoint.Endpoint, error) {
		return e, nil
	}

	machines := []string{"http://127.0.0.1:2379"}
	client := etcd.NewClient(machines)
	p, err := NewPublisher(client, "/stream", factory, logger)
	if err != nil {
		t.Fatalf("failed to create new publisher: %v", err)
	}
	defer p.Stop()

	if _, err := p.Endpoints(); err != nil {
		t.Fatal(err)
	}
}

func TestBadFactory(t *testing.T) {
	var (
		logger = log.NewNopLogger()
		e      = func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil }
	)

	factory := func(instance string) (endpoint.Endpoint, error) {
		return e, errors.New("_")
	}

	machines := []string{"http://127.0.0.1:2379"}
	client := etcd.NewClient(machines)
	p, err := NewPublisher(client, "/stream", factory, logger)
	if err != nil {
		t.Fatalf("failed to create new publisher: %v", err)
	}
	defer p.Stop()

	endpoints, err := p.Endpoints()
	if err != nil {
		t.Fatal(err)
	}
	if want, have := 0, len(endpoints); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}

func TestRefreshWithChange(t *testing.T) {
	var (
		logger = log.NewLogfmtLogger(os.Stderr)
		e      = func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil }
	)

	factory := func(instance string) (endpoint.Endpoint, error) {
		return e, nil
	}

	machines := []string{"http://127.0.0.1:2379"}
	client := etcd.NewClient(machines)
	p, err := NewPublisher(client, "/stream", factory, logger)
	if err != nil {
		t.Fatalf("failed to create new publisher: %v", err)
	}
	defer p.Stop()
	p.Endpoints()
	select {}
}

func TestErrPublisherStopped(t *testing.T) {
	var (
		logger = log.NewNopLogger()
		e      = func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil }
	)

	factory := func(instance string) (endpoint.Endpoint, error) {
		return e, errors.New("_")
	}

	machines := []string{"http://127.0.0.1:2379"}
	client := etcd.NewClient(machines)
	p, err := NewPublisher(client, "/stream", factory, logger)
	if err != nil {
		t.Fatalf("failed to create new publisher: %v", err)
	}

	p.Stop()

	_, have := p.Endpoints()
	if want := loadbalancer.ErrPublisherStopped; want != have {
		t.Fatalf("want %v, have %v", want, have)
	}
}

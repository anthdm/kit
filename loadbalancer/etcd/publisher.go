package etcd

import (
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/log"
)

type Publisher struct {
	client    *etcd.Client
	keyspace  string
	factory   loadbalancer.Factory
	logger    log.Logger
	endpoints chan []endpoint.Endpoint
	quit      chan struct{}
}

func NewPublisher(c *etcd.Client, k string, f loadbalancer.Factory, logger log.Logger) (*Publisher, error) {
	logger = log.NewContext(logger).With("component", "Etcd Publisher")

	p := &Publisher{
		client:    c,
		keyspace:  k,
		factory:   f,
		logger:    logger,
		endpoints: make(chan []endpoint.Endpoint),
		quit:      make(chan struct{}),
	}

	instances, err := p.getEntries()
	if err != nil {
		return nil, err
	}
	go p.loop(makeEndpoints(instances, f, logger))
	return p, nil
}

func (p *Publisher) loop(endpoints []endpoint.Endpoint) {
	watchChan := make(chan *etcd.Response)
	go p.client.Watch(p.keyspace, 0, true, watchChan, nil)

	for {
		select {
		case p.endpoints <- endpoints:
			fmt.Println("initialized endpoints")

		case <-watchChan:
			instances, err := p.getEntries()
			if err != nil {
				p.logger.Log("err", err)
				continue
			}
			p.endpoints <- makeEndpoints(instances, p.factory, p.logger)

		case <-p.quit:
			return
		}
	}
}

func (p *Publisher) Endpoints() ([]endpoint.Endpoint, error) {
	select {
	case endpoints := <-p.endpoints:
		return endpoints, nil
	case <-p.quit:
		return nil, loadbalancer.ErrPublisherStopped
	}
}

func (p *Publisher) getEntries() ([]string, error) {
	resp, err := p.client.Get(p.keyspace, false, true)
	if err != nil {
		return nil, err
	}

	instances := make([]string, len(resp.Node.Nodes))
	for i, node := range resp.Node.Nodes {
		n, err := p.client.Get(node.Key, false, true)
		if err != nil {
			return nil, err
		}
		instances[i] = n.Node.Value
	}
	p.logger.Log("endpoints fetched", fmt.Sprint(instances))
	return instances, nil
}

func (p *Publisher) Stop() {
	close(p.quit)
}

func makeEndpoints(addrs []string, f loadbalancer.Factory, logger log.Logger) []endpoint.Endpoint {
	endpoints := make([]endpoint.Endpoint, 0, len(addrs))

	for _, addr := range addrs {
		endpoint, err := f(addr)
		if err != nil {
			logger.Log("instance", addr, "err", err)
			continue
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

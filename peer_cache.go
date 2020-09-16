package plugin_bustlinker

import (
	"context"
	"fmt"
	config "github.com/glvd/plugin-bustlinker/config"
	core "github.com/ipfs/go-ipfs/core"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

const peerName = "address"

type PeerCache struct {
	lock      *sync.RWMutex
	addresses map[peer.ID]peer.AddrInfo
	cache     Cacher
}

func peerCache() *PeerCache {
	return &PeerCache{
		lock:      &sync.RWMutex{},
		addresses: make(map[peer.ID]peer.AddrInfo),
	}
}

func NewAddress(cfg *config.Config, node *core.IpfsNode) *PeerCache {
	cache := peerCache()
	log.Info("peer cache initialized")

	cache.cache = NewCache(cfg.Address, cfg.Repo, peerName)
	return cache
}

func (c *PeerCache) CheckPeerAddress(id peer.ID) (b bool) {
	c.lock.RLock()
	_, b = c.addresses[id]
	c.lock.RUnlock()
	return
}

func (c *PeerCache) AddPeerAddress(addr peer.AddrInfo) (b bool) {
	c.lock.RLock()
	_, b = c.addresses[addr.ID]
	c.lock.RUnlock()
	if b {
		return !b
	}
	c.lock.Lock()
	_, b = c.addresses[addr.ID]
	if !b {
		c.addresses[addr.ID] = addr
	}
	c.lock.Unlock()
	return !b
}

func (c *PeerCache) GetAddress(id peer.ID) (ai peer.AddrInfo, b bool) {
	c.lock.RLock()
	ai, b = c.addresses[id]
	c.lock.RUnlock()
	return ai, b
}

func (c *PeerCache) Peers() (ids []peer.ID) {
	c.lock.RLock()
	for id := range c.addresses {
		ids = append(ids, id)
	}
	c.lock.RUnlock()
	return
}

func (c *PeerCache) UpdatePeerAddress(new peer.AddrInfo) bool {
	address, b := c.GetAddress(new.ID)
	if !b {
		return c.AddPeerAddress(new)
	}

	updated := false
	newAddr := address
	mark := make(map[fmt.Stringer]bool)
	for _, addr := range address.Addrs {
		mark[addr] = true
	}

	for _, addr := range new.Addrs {
		if !mark[addr] {
			updated = true
			newAddr.Addrs = append(newAddr.Addrs, addr)
		}
	}

	if !updated {
		return updated
	}

	c.lock.Lock()
	c.addresses[new.ID] = newAddr
	c.lock.Unlock()
	return updated
}

func (c *PeerCache) LoadAddress(ctx context.Context) (<-chan peer.AddrInfo, error) {
	ai := make(chan peer.AddrInfo)
	go func() {
		defer close(ai)

		c.cache.Range(func(hash string, value string) bool {
			log.Infow("per cache ranging", "hash", hash, "value", value)
			var info peer.AddrInfo
			err := info.UnmarshalJSON([]byte(value))
			if err != nil {
				log.Errorw("unmarshal addr info failed in range", "err", err)
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case ai <- info:
				return true
			}
		})
	}()
	return ai, nil
}

// SaveNode ...
func (c *PeerCache) SaveAddress(ctx context.Context) (err error) {
	go func() {
		for _, id := range c.Peers() {
			select {
			case <-ctx.Done():
				return
			default:
				address, b := c.GetAddress(id)
				if !b {
					continue
				}
				err := c.cache.Store(id.Pretty(), address)
				if err != nil {
					return
				}
			}
		}
	}()
	return nil
}

package linker

import (
	"context"
	"sync"

	core "github.com/ipfs/go-ipfs/core"
	coreapi "github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"go.uber.org/atomic"
)

type Pinning interface {
	Get() []string
	Clear()
	AddSync(pin string)
	Add(pin string)
	Set(pins []string)
}

type pinning struct {
	ctx      context.Context
	cancel   context.CancelFunc
	running  *atomic.Bool
	syncing  *sync.Pool
	node     *core.IpfsNode
	pins     map[string]bool
	pinsLock *sync.RWMutex
}

func (p *pinning) Get() []string {
	var pins []string
	p.pinsLock.RLock()
	if len(p.pins) == 0 {
		return pins
	}
	for pin := range p.pins {
		pins = append(pins, pin)
	}
	p.pinsLock.RUnlock()
	return pins
}

func (p *pinning) Clear() {
	p.pinsLock.Lock()
	p.pins = make(map[string]bool)
	p.pinsLock.Unlock()
}

func (p *pinning) AddSync(pin string) {
	p.syncing.Put(pin)
	if !p.running.Load() {
		p.Resume()
	}
}

func (p *pinning) Add(pin string) {
	p.pinsLock.Lock()
	p.pins[pin] = true
	p.pinsLock.Unlock()
}

func (p *pinning) Set(pins []string) {
	ps := make(map[string]bool, len(pins))
	for _, pin := range pins {
		ps[pin] = true
	}
	p.pinsLock.Lock()
	p.pins = ps
	p.pinsLock.Unlock()
}

func (p *pinning) Pause() {
	p.running.Store(false)
	if p.running.CAS(true, false) {
		p.cancel()
	}
}

func (p *pinning) Resume() {
	if p.running.CAS(false, true) {
		go p.run()
	}
}

func (p *pinning) run() {
	defer p.Pause()
	p.ctx, p.cancel = context.WithCancel(context.TODO())
	api, err := coreapi.NewCoreAPI(p.node)
	if err != nil {
		log.Error("failed get core api on pinning:", err)
		return
	}
	var pstr string
	var b bool
	var newPath path.Path
	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			return
		default:
			v := p.syncing.Get()
			if v == nil {
				return
			}
			pstr, b = v.(string)
			if !b {
				continue
			}

			newPath = path.New(pstr)
			typ, b2, err := api.Pin().IsPinned(p.ctx, newPath)
			log.Info("check pin hash", "hash", pstr, "exist", b2, "error", err)
			if (b2 && typ == "recursive") || err != nil {
				continue
			}
			err = api.Pin().Add(p.ctx, newPath)
			if err != nil {
				continue
			}
			p.Add(pstr)
		}
	}
}

func newPinning(node *core.IpfsNode) Pinning {
	p := &pinning{
		running:  atomic.NewBool(true),
		node:     node,
		syncing:  &sync.Pool{},
		pins:     make(map[string]bool),
		pinsLock: &sync.RWMutex{},
	}
	go p.run()
	return p
}

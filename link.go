package linker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/glvd/go-linker/config"
	"sync"
	"time"

	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/multiformats/go-multiaddr"
)

const Version = "0.0.1"
const LinkPeers = "/link" + "/peers/" + Version
const LinkAddress = "/link" + "/address/" + Version
const LinkHash = "/link" + "/hash/" + Version

var protocols = []string{
	LinkPeers,
	LinkAddress,
}

var NewLine = []byte{'\n'}

type Linker interface {
	SetNode(node *core.IpfsNode) Linker
	Start() error
	//plugin.Plugin
	//plugin.PluginDaemonInternal
}

type link struct {
	ctx         context.Context
	cfg         *config.Config
	node        *core.IpfsNode
	failedCount map[peer.ID]int64
	failedLock  *sync.RWMutex

	addresses *PeerCache
	hashes    *HashCache
	pinning   Pinning
	repo      string
}

func (l *link) SetNode(node *core.IpfsNode) Linker {
	l.node = node
	return l
}

func (l *link) Syncing() {
	go l.syncPin()
	for {
		wg := &sync.WaitGroup{}
		for _, conn := range l.node.PeerHost.Network().Conns() {
			if l.node.Identity == conn.RemotePeer() {
				continue
			}
			wg.Add(1)
			go l.getRemotePeerAddress(wg, conn)
			wg.Add(1)
			go l.getRemoteHash(wg, conn)
		}
		wg.Wait()
		log.Debug("waiting for next loop")
		time.Sleep(30 * time.Second)
	}
}

func checkAddrExist(addrs []multiaddr.Multiaddr, addr multiaddr.Multiaddr) bool {
	for i := range addrs {
		if addr.Equal(addrs[i]) {
			return true
		}
	}
	return false
}

func (l *link) newLinkPeersHandle() (protocol.ID, func(stream network.Stream)) {
	return LinkPeers, func(stream network.Stream) {
		log.Debug("link peer called")
		var err error
		defer stream.Close()
		remoteID := stream.Conn().RemotePeer()

		if !checkAddrExist(l.node.Peerstore.Addrs(remoteID), stream.Conn().RemoteMultiaddr()) {
			l.node.Peerstore.AddAddr(remoteID, stream.Conn().RemoteMultiaddr(), 7*24*time.Hour)
		}

		peers := l.node.PeerHost.Network().Peers()
		log.Infow("get all peers", "total", len(peers))
		for _, peer := range peers {
			info := l.node.Peerstore.PeerInfo(peer)
			json, _ := info.MarshalJSON()
			_, err = stream.Write(json)
			if err != nil {
				log.Debugw("stream write error", "error", err)
				return
			}
			_, err = stream.Write(NewLine)
			if err != nil {
				log.Debugw("stream write error", "error", err)
				return
			}
			log.Debugw("peer sent success", "to", remoteID, stream.Conn().RemoteMultiaddr().String(), "addr", info.String())
		}
	}
}

func (l *link) newLinkHashHandle() (protocol.ID, func(stream network.Stream)) {
	return LinkHash, func(stream network.Stream) {
		log.Debug("link hash called")
		var err error
		defer stream.Close()
		for _, peer := range l.pinning.Get() {
			_, err = stream.Write([]byte(peer))
			if err != nil {
				log.Debugw("stream write error", "error", err)
				return
			}
			_, err = stream.Write(NewLine)
			if err != nil {
				log.Debugw("stream write error", "error", err)
				return
			}
		}
	}
}

func (l *link) registerHandle() {
	l.node.PeerHost.SetStreamHandler(l.newLinkPeersHandle())
	l.node.PeerHost.SetStreamHandler(l.newLinkHashHandle())
}

func (l *link) Start() error {
	fmt.Println("Link start")

	l.pinning = newPinning(l.node)
	l.addresses = NewAddress(l.cfg, l.node)
	l.hashes = NewHash(l.cfg, l.node)

	l.registerHandle()
	to := time.Duration(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.TODO(), to)
	defer cancel()
	address, err := l.addresses.LoadAddress(ctx)
	if err != nil {
		return err
	}
	t := time.NewTimer(to)
UpdateCase:
	for {
		select {
		case <-t.C:
			break UpdateCase
		case <-ctx.Done():
			break UpdateCase
		default:
			l.addresses.UpdatePeerAddress(<-address)
		}
	}
	go l.Syncing()
	return nil
}

func (l *link) getRemotePeerAddress(wg *sync.WaitGroup, conn network.Conn) {
	defer wg.Done()
	s, err := l.node.PeerHost.NewStream(l.ctx, conn.RemotePeer(), LinkPeers)
	if err != nil {
		return
	}
	defer s.Close()

	reader := bufio.NewReader(s)
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
			line, _, err := reader.ReadLine()
			if err != nil {
				return
			}
			ai := peer.AddrInfo{}
			err = ai.UnmarshalJSON(line)
			if err != nil {
				log.Error("unmarlshal json failed", "line", string(line), "error", err)
				return
			}
			if ai.ID == l.node.Identity {
				continue
			}
			log.Debugw("receive address", "from", conn.RemotePeer().Pretty(), "addrinfo", ai.String(), "addr size", len(ai.Addrs))
			if err := l.UpdatePeerAddress(ai); err != nil {
				log.Error("update peer address failed:", err)
			}
		}
	}
}

func (l *link) getAddCount(id peer.ID) int64 {
	count := int64(0)
	l.failedLock.Lock()
	count, l.failedCount[id] = l.failedCount[id], l.failedCount[id]+1
	l.failedLock.Unlock()
	return count
}

func (l *link) UpdatePeerAddress(ai peer.AddrInfo) error {
	stream, err := l.node.PeerHost.NewStream(l.ctx, ai.ID, LinkPeers)
	if err == nil {
		stream.Close()
		return nil
	}
	log.Debug("stream connect failed:", err)
	if l.addresses.UpdatePeerAddress(ai) {
		count := l.getAddCount(ai.ID)
		if count > l.cfg.MaxAttempts {
			return errors.New("connect failed max")
		}
		api, err := coreapi.NewCoreAPI(l.node)
		if err != nil {
			return err
		}
		err = api.Swarm().Connect(l.ctx, ai)
		if err != nil {
			return err
		}
		log.Infow("connect success", "remote", ai.String())
	}
	return nil
}

func (l *link) getRemoteHash(wg *sync.WaitGroup, conn network.Conn) {
	defer wg.Done()
	id := conn.RemotePeer()
	s, err := l.node.PeerHost.NewStream(l.ctx, id, LinkHash)
	if err != nil {
		return
	}
	defer s.Close()

	reader := bufio.NewReader(s)
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
			line, _, err := reader.ReadLine()
			if err != nil {
				return
			}
			log.Debugw("received remote hash", "from", id, "hash", string(line))
			l.UpdateHash(path.New(string(line)), id)
		}
	}
}

func (l *link) UpdateHash(hash path.Path, id peer.ID) {
	l.hashes.Add(hash.String(), id)
	l.pinning.AddSync(hash.String())
}

func (l *link) syncPin() {
	api, err := coreapi.NewCoreAPI(l.node)
	if err != nil {
		return
	}

	t := time.NewTimer(time.Duration(l.cfg.Pinning.PerSeconds) * time.Second)
	var pin iface.Pin
	for {
		select {
		case <-t.C:
			ls, err := api.Pin().Ls(l.ctx, options.Pin.Ls.Recursive())
			if err != nil {
				return
			}
			for pin = range ls {
				log.Debugw("add to pin cache", "hash", pin.Path().String())
				l.pinning.Add(pin.Path().String())
			}
			//release:per/day:test:60*sec
			t.Reset(60 * time.Second)
		}
	}
}

func New(repo string, cfg interface{}) (Linker, error) {
	v, b := cfg.(*config.Config)
	if !b {
		return nil, errors.New("could not transfer config")
	}
	return &link{
		repo:        repo,
		cfg:         v,
		failedCount: make(map[peer.ID]int64),
		failedLock:  &sync.RWMutex{},
		//pinning:     newPinning(node),
		//addresses:   NewAddress(cfg, node),
		//hashes:      NewHash(cfg, node),
	}, nil
}

func New2(ctx context.Context, cfg *config.Config, node *core.IpfsNode) Linker {
	return &link{
		ctx:         ctx,
		cfg:         cfg,
		node:        node,
		failedCount: make(map[peer.ID]int64),
		failedLock:  &sync.RWMutex{},
		pinning:     newPinning(node),
		addresses:   NewAddress(cfg, node),
		hashes:      NewHash(cfg, node),
	}
}

var _ Linker = &link{}

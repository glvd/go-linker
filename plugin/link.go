package plugin

import (
	linker "github.com/glvd/go-bustlinker"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/plugin"
)

type bustlinker struct {
	lnk linker.Linker
}

func (b *bustlinker) Name() string {
	return "linker"
}

func (b *bustlinker) Version() string {
	return "0.0.1"
}

func (b *bustlinker) Init(env *plugin.Environment) error {
	l, err := linker.New(env.Repo, env.Config)
	if err != nil {
		return err
	}
	b.lnk = l
	return nil
}

func (b *bustlinker) Start(node *core.IpfsNode) error {
	return b.lnk.SetNode(node).Start()
}

package plugin

import (
	linker "github.com/glvd/go-linker"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/plugin"
)

var Plugins = []plugin.Plugin{
	&linkerPlugin{},
}

type linkerPlugin struct {
	lnk linker.Linker
}

func (b *linkerPlugin) Name() string {
	return "linker"
}

func (b *linkerPlugin) Version() string {
	return "0.0.1"
}

func (b *linkerPlugin) Init(env *plugin.Environment) error {
	l, err := linker.New(env.Repo, env.Config)
	if err != nil {
		return err
	}
	b.lnk = l
	return nil
}

func (b *linkerPlugin) Start(node *core.IpfsNode) error {
	return b.lnk.SetNode(node).Start()
}

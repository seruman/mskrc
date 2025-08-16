package kclconfig

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
	"code.selman.me/mskrc/internal"
	kclclient "github.com/twmb/kcl/client"
)

type (
	KclConfig = kclclient.Cfg
)

type Config struct {
	cfg     kclclient.Cfg
	cluster internal.Cluster
}

func (c Config) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	preamble := fmt.Sprintf("# name: %v", c.cluster.Name)
	if c.cluster.Alias != "" {
		preamble = fmt.Sprintf("# name: %v, alias: %v", c.cluster.Name, c.cluster.Alias)
	}

	fmt.Fprintln(&buf, preamble)
	if err := toml.NewEncoder(&buf).Encode(c.cfg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func New(clusters []internal.Cluster) []Config {
	var configs []Config
	for _, c := range clusters {
		configs = append(
			configs,
			Config{
				cfg: KclConfig{
					SeedBrokers: c.Brokers,
					// TODO(selman): make this configurable
					TimeoutMillis: 10000,
				},
				cluster: c,
			},
		)
	}

	return configs
}

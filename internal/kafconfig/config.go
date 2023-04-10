package kafconfig

import (
	"bytes"
	"fmt"

	"github.com/birdayz/kaf/pkg/config"
	"gopkg.in/yaml.v2"

	"github.com/seruman/mskrc/internal"
)

type (
	KafConfig  = config.Config
	KafCluster = config.Cluster
)

type Config struct {
	cfg KafConfig
}

func (c Config) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	if err := encoder.Encode(c.cfg); err != nil {
		return nil, fmt.Errorf("yaml encode: %w", err)
	}

	return buf.Bytes(), nil
}

func New(clusters []internal.Cluster) Config {
	var clustercfgs []*KafCluster
	for _, c := range clusters {

		name := c.Name
		if c.Alias != "" {
			name = c.Alias
		}

		clustercfgs = append(clustercfgs, &KafCluster{
			Name:    name,
			Brokers: c.Brokers,
			Version: c.Version,
		})
	}

	return Config{cfg: KafConfig{Clusters: clustercfgs}}
}

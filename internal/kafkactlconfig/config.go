package kafkactlconfig

import (
	"bytes"
	"fmt"

	"github.com/seruman/mskrc/internal"
	"gopkg.in/yaml.v3"
)

// NOTE: kafkactl does not expose any type to be used as configuration.

type KafkactlConfig struct {
	Contexts       map[string]kafkactlContext
	CurrentContext string
}

type kafkactlContext struct {
	Brokers []string
}

type Config struct {
	cfg KafkactlConfig
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
	contexts := make(map[string]kafkactlContext)
	for _, c := range clusters {
		name := c.Name
		if c.Alias != "" {
			name = c.Alias
		}

		contexts[name] = kafkactlContext{Brokers: c.Brokers}
	}

	return Config{cfg: KafkactlConfig{Contexts: contexts}}
}

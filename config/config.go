package config

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type CacheConfig struct {
	BackupSeconds int
}

type Pinning struct {
	PerSeconds int
}

type Config struct {
	MaxAttempts int64
	Pinning     Pinning
	Hash        CacheConfig
	Address     CacheConfig
	Repo        string
}

var DefaultBootstrapAddresses = []string{}
var DefaultPinningSeconds = 30

// Clone copies the config. Use when updating.
func (c *Config) Clone() (*Config, error) {
	var newConfig Config
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(c); err != nil {
		return nil, fmt.Errorf("failure to encode config: %s", err)
	}

	if err := json.NewDecoder(&buf).Decode(&newConfig); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}

	return &newConfig, nil
}

func FromMap(v map[string]interface{}) (*Config, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	var conf Config
	if err := json.NewDecoder(buf).Decode(&conf); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}
	return &conf, nil
}

func ToMap(cfg *Config) (map[string]interface{}, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(cfg); err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.NewDecoder(buf).Decode(&m); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}
	return m, nil
}

func InitConfig(repoPath string) (*Config, error) {
	cfg := Config{
		Repo:        repoPath,
		MaxAttempts: 0,
		Pinning: Pinning{
			PerSeconds: DefaultPinningSeconds,
		},
		Hash: CacheConfig{
			BackupSeconds: 0,
		},
		Address: CacheConfig{
			BackupSeconds: 0,
		},
	}
	return &cfg, nil
}

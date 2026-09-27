package amqp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func LoadConfigFile(path string) (*AMQP, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	fileExt := filepath.Ext(path)
	cfg := AMQP{}

	switch fileExt {
	case ".json":
		{
			decoder := json.NewDecoder(fd)
			err = decoder.Decode(&cfg)
		}

	case ".yaml":
		{
			decoder := yaml.NewDecoder(fd)
			err = decoder.Decode(&cfg)
		}
	default:
		return nil, fmt.Errorf("the passed config file has to be either json or yaml")
	}

	if err := validateAMQP(&cfg); err != nil {
		return nil, err
	}
	return &cfg, err
}

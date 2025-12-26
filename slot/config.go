package slot

import (
	"errors"
	"strings"
	"time"
)

type Config struct {
	Name                        string
	SlotActivityCheckerInterval time.Duration
	CreateIfNotExists           bool
}

type Option func(*Config)

func NewConfig(opts ...Option) Config {
	c := Config{}
	for _, opt := range opts {
		opt(&c)
	}

	return c
}

func WithName(name string) Option {
	return func(c *Config) {
		c.Name = name
	}
}

func WithSlotActivityCheckerInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.SlotActivityCheckerInterval = interval
	}
}

func WithCreateIfNotExists(createIfNotExists bool) Option {
	return func(c *Config) {
		c.CreateIfNotExists = createIfNotExists
	}
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	if c.SlotActivityCheckerInterval < 1000 {
		err = errors.Join(err, errors.New("slot activity checker interval cannot be lower than 1000 ms"))
	}

	return err
}

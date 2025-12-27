package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snapflowio/cdc/publication"
	"github.com/snapflowio/cdc/slot"
)

type Config struct {
	Heartbeat   HeartbeatConfig
	Logger      LoggerConfig
	Host        string
	Username    string
	Password    string
	Database    string
	Schema      string
	Publication publication.Config
	Slot        slot.Config
	Snapshot    SnapshotConfig
	Port        int
	DebugMode   bool
}

type LoggerConfig struct {
	LogLevel logrus.Level
}

type HeartbeatConfig struct {
	Table publication.Table
}

type Option func(*Config)

func NewConfig(opts ...Option) *Config {
	c := &Config{}
	for _, opt := range opts {
		opt(c)
	}
	c.SetDefault()
	return c
}

func WithDSN(dsn string) Option {
	return func(c *Config) {
		parsedURL, err := url.Parse(dsn)
		if err != nil {
			return
		}

		c.Host = parsedURL.Hostname()
		if parsedURL.Port() != "" {
			port := 5432
			if _, err := fmt.Sscanf(parsedURL.Port(), "%d", &port); err == nil {
				c.Port = port
			}
		}

		if parsedURL.User != nil {
			c.Username = parsedURL.User.Username()
			if password, ok := parsedURL.User.Password(); ok {
				c.Password = password
			}
		}

		c.Database = strings.TrimPrefix(parsedURL.Path, "/")
	}
}

func WithSchema(schema string) Option {
	return func(c *Config) {
		c.Schema = schema
	}
}

func WithLogLevel(level logrus.Level) Option {
	return func(c *Config) {
		c.Logger.LogLevel = level
	}
}

func WithDebugMode(enabled bool) Option {
	return func(c *Config) {
		c.DebugMode = enabled
	}
}

func WithPublication(pubConfig publication.Config) Option {
	return func(c *Config) {
		c.Publication = pubConfig
	}
}

func WithSlot(slotConfig slot.Config) Option {
	return func(c *Config) {
		c.Slot = slotConfig
	}
}

func WithSnapshot(snapshotConfig SnapshotConfig) Option {
	return func(c *Config) {
		c.Snapshot = snapshotConfig
	}
}

func WithHeartbeat(heartbeatConfig HeartbeatConfig) Option {
	return func(c *Config) {
		c.Heartbeat = heartbeatConfig
	}
}

func WithHeartbeatTable(table publication.Table) Option {
	return func(c *Config) {
		c.Heartbeat.Table = table
	}
}

func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) ReplicationDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) SetDefault() {
	if c.Schema == "" {
		c.Schema = "public"
	}

	if c.Port == 0 {
		c.Port = 5432
	}

	// Set heartbeat table schema to global schema if not explicitly set or still default "public"
	if c.Heartbeat.Table.Name != "" && (c.Heartbeat.Table.Schema == "" || c.Heartbeat.Table.Schema == "public") {
		c.Heartbeat.Table.Schema = c.Schema
	}

	if c.Slot.SlotActivityCheckerInterval == 0 {
		c.Slot.SlotActivityCheckerInterval = 1000
	}

	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" || table.Schema == "public" {
			c.Publication.Tables[tableID].Schema = c.Schema
		}
	}

	if c.Snapshot.Enabled {
		if c.Snapshot.Mode == "" {
			c.Snapshot.Mode = SnapshotModeNever
		}
		if c.Snapshot.ChunkSize == 0 {
			c.Snapshot.ChunkSize = 8_000
		}
		if c.Snapshot.ClaimTimeout == 0 {
			c.Snapshot.ClaimTimeout = 30 * time.Second
		}
		if c.Snapshot.HeartbeatInterval == 0 {
			c.Snapshot.HeartbeatInterval = 5 * time.Second
		}

		for tableID, table := range c.Snapshot.Tables {
			if table.Schema == "" || table.Schema == "public" {
				c.Snapshot.Tables[tableID].Schema = c.Schema
			}
		}
	}
}

func (c *Config) IsSnapshotOnlyMode() bool {
	return c.Snapshot.Enabled && c.Snapshot.Mode == SnapshotModeSnapshotOnly
}

func (c *Config) GetSnapshotTables(publicationInfo *publication.Config) (publication.Tables, error) {
	if c.IsSnapshotOnlyMode() {
		if len(c.Snapshot.Tables) == 0 {
			return nil, errors.New("snapshot.tables must be specified for snapshot_only mode")
		}
		return c.Snapshot.Tables, nil
	}

	if len(c.Snapshot.Tables) > 0 {
		return c.validateSnapshotSubset(publicationInfo.Tables)
	}

	return publicationInfo.Tables, nil
}

func (c *Config) validateSnapshotSubset(pubTables publication.Tables) (publication.Tables, error) {
	if len(pubTables) == 0 {
		return nil, errors.New("publication has no tables defined")
	}

	pubMap := make(map[string]publication.Table)
	for _, t := range pubTables {
		key := t.Schema + "." + t.Name
		pubMap[key] = t
	}

	validatedTables := make(publication.Tables, 0, len(c.Snapshot.Tables))
	for _, st := range c.Snapshot.Tables {
		key := st.Schema + "." + st.Name
		pubTable, exists := pubMap[key]
		if !exists {
			return nil, fmt.Errorf("snapshot table '%s' not found in publication '%s'", key, c.Publication.Name)
		}
		validatedTables = append(validatedTables, pubTable)
	}

	return validatedTables, nil
}

func (c *Config) Validate() error {
	var err error
	if isEmpty(c.Host) {
		err = errors.Join(err, errors.New("host cannot be empty"))
	}

	if isEmpty(c.Username) {
		err = errors.Join(err, errors.New("username cannot be empty"))
	}

	if isEmpty(c.Password) {
		err = errors.Join(err, errors.New("password cannot be empty"))
	}

	if isEmpty(c.Database) {
		err = errors.Join(err, errors.New("database cannot be empty"))
	}

	if !c.IsSnapshotOnlyMode() {
		if cErr := c.Publication.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}

		if cErr := c.Slot.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if cErr := c.Snapshot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	// Validate snapshot with empty tables
	if c.Snapshot.Enabled && len(c.Publication.Tables) == 0 {
		err = errors.Join(err, errors.New("snapshot cannot be enabled with empty publication tables, either disable snapshot for dynamic table management, or add initial tables to the publication"))
	}

	if isEmpty(c.Heartbeat.Table.Name) {
		err = errors.Join(err, errors.New("heartbeat.table.name cannot be empty"))
	}

	return err
}

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	fmt.Printf("Config: Host=%s Port=%d Database=%s Username=%s\n", cfg.Host, cfg.Port, cfg.Database, cfg.Username)
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

type SnapshotConfig struct {
	Mode              SnapshotMode
	InstanceID        string
	Tables            publication.Tables
	ChunkSize         int64
	ClaimTimeout      time.Duration
	HeartbeatInterval time.Duration
	Enabled           bool
}

type SnapshotOption func(*SnapshotConfig)

func NewSnapshotConfig(opts ...SnapshotOption) SnapshotConfig {
	sc := SnapshotConfig{}
	for _, opt := range opts {
		opt(&sc)
	}
	return sc
}

func WithSnapshotEnabled(enabled bool) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.Enabled = enabled
	}
}

func WithSnapshotMode(mode SnapshotMode) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.Mode = mode
	}
}

func WithSnapshotInstanceID(instanceID string) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.InstanceID = instanceID
	}
}

func WithSnapshotTables(tables publication.Tables) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.Tables = tables
	}
}

func WithSnapshotChunkSize(chunkSize int64) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.ChunkSize = chunkSize
	}
}

func WithSnapshotClaimTimeout(timeout time.Duration) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.ClaimTimeout = timeout
	}
}

func WithSnapshotHeartbeatInterval(interval time.Duration) SnapshotOption {
	return func(sc *SnapshotConfig) {
		sc.HeartbeatInterval = interval
	}
}

func (s *SnapshotConfig) Validate() error {
	if !s.Enabled {
		return nil
	}

	validModes := []SnapshotMode{SnapshotModeInitial, SnapshotModeNever, SnapshotModeSnapshotOnly}
	isValid := false
	for _, mode := range validModes {
		if s.Mode == mode {
			isValid = true
			break
		}
	}
	if !isValid {
		return errors.New("snapshot mode must be 'initial', 'never', or 'snapshot_only'")
	}

	if s.ChunkSize <= 0 {
		return errors.New("snapshot chunk size must be greater than 0")
	}
	if s.ClaimTimeout <= 0 {
		return errors.New("snapshot claim timeout must be greater than 0")
	}
	if s.HeartbeatInterval <= 0 {
		return errors.New("snapshot heartbeat interval must be greater than 0")
	}

	if s.Mode == SnapshotModeSnapshotOnly && len(s.Tables) == 0 {
		return errors.New("snapshot.tables must be specified for snapshot_only mode")
	}

	return nil
}

type SnapshotMode string

const (
	SnapshotModeInitial      SnapshotMode = "initial"
	SnapshotModeNever        SnapshotMode = "never"
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
)

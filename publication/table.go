package publication

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type Table struct {
	Name            string
	ReplicaIdentity string
	Schema          string
	IndexName       string
}

type TableOption func(*Table)

func NewTable(name string, opts ...TableOption) Table {
	t := Table{
		Name:            name,
		ReplicaIdentity: ReplicaIdentityDefault,
		Schema:          "public",
	}

	for _, opt := range opts {
		opt(&t)
	}

	return t
}

func WithSchema(schema string) TableOption {
	return func(t *Table) {
		t.Schema = schema
	}
}

func WithReplicaIdentity(replicaIdentity string) TableOption {
	return func(t *Table) {
		t.ReplicaIdentity = replicaIdentity
	}
}

func WithIndexName(indexName string) TableOption {
	return func(t *Table) {
		t.IndexName = indexName
	}
}

func (t Table) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, t.ReplicaIdentity) {
		return fmt.Errorf("undefined replica identity option. valid identity options are: %v", ReplicaIdentityOptions)
	}

	if t.ReplicaIdentity == ReplicaIdentityIndex && strings.TrimSpace(t.IndexName) == "" {
		return errors.New("index name must be provided when using INDEX replica identity")
	}

	return nil
}

type Tables []Table

func (ts Tables) Validate() error {
	for _, t := range ts {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (ts Tables) Diff(tss Tables) Tables {
	res := Tables{}
	tssMap := make(map[string]Table)

	for _, t := range tss {
		tssMap[t.Name+t.ReplicaIdentity] = t
	}

	for _, t := range ts {
		if v, found := tssMap[t.Name+t.ReplicaIdentity]; !found || v.ReplicaIdentity != t.ReplicaIdentity {
			res = append(res, t)
		}
	}

	return res
}

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEngineListed(t *testing.T) {
	tests := []struct {
		engine string
		want   bool
	}{
		{engine: "MergeTree", want: true},
		{engine: "ReplicatedAggregatingMergeTree", want: true},
		{engine: "Buffer", want: true},
		{engine: "Null", want: true},
		{engine: "Distributed", want: true},
		{engine: "MaterializedView", want: true},
		{engine: "Memory", want: false},
		{engine: "Merge", want: false},
		{engine: "S3", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.engine, func(t *testing.T) {
			assert.Equal(t, tt.want, engineListed(tt.engine, tableEngines))
		})
	}
}

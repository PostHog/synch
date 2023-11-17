package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadSkipQueries(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     []string
		wantErr  bool
	}{
		{
			name:     "valid file",
			filePath: "testdata/skip_queries.sql",
			want:     []string{"SELECT * FROM table1", "SELECT * FROM table2"},
			wantErr:  false,
		},
		{
			name:     "invalid file",
			filePath: "testdata/invalid.csv",
			want:     nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadSkipQueries(tt.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadSkipQueries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

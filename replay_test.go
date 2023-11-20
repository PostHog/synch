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

func TestBuildQueryLogQuery(t *testing.T) {
	tests := []struct {
		name        string
		skipQueries []string
		want        string
		wantErr     bool
	}{
		{
			name:        "valid queries",
			skipQueries: []string{"SELECT * FROM table1", "SELECT * FROM table2"},
			want: `
select normalized_query_hash from system.query_log
where type = 2 and is_initial_query = 1 and query_kind = 'Select'
and query_start_time >= {start:String} and query_start_time <= {stop:String} 
and (query like '%SELECT * FROM table1%' or query like '%SELECT * FROM table2%')
group by normalized_query_hash
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildQueryLogQuery(tt.skipQueries)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildQueryLogQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

package es

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre/es/app"
	"github.com/square/etre/es/config"
)

func TestParsePatches(t *testing.T) {
	testCases := []struct {
		name   string
		raw    []string
		parsed map[string]string
		err    bool
		strict bool
	}{
		{name: "happy path", raw: []string{"a=1", "b=2", "c=3"}, parsed: map[string]string{"a": "1", "b": "2", "c": "3"}},
		{name: "whitespace around value", raw: []string{"a= 1 ", "b= 2 ", "c=\t3\t"}, parsed: map[string]string{"a": "1", "b": "2", "c": "3"}},
		{name: "whitespace around label", raw: []string{" a=1", " b =2", "\tc\t=3"}, parsed: map[string]string{"a": "1", "b": "2", "c": "3"}},
		{name: "value contains whitespace", raw: []string{"a= 1 2 "}, parsed: map[string]string{"a": "1 2"}},
		{name: "value contains equals", raw: []string{"a=1=2"}, parsed: map[string]string{"a": "1=2"}},
		{name: "empty value", raw: []string{"a="}, parsed: map[string]string{"a": ""}},
		{name: "missing label", raw: []string{"=1"}, err: true},
		{name: "missing label and value", raw: []string{"="}, err: true},
		{name: "empty string", raw: []string{""}, err: true},
		{name: "strict label whitespace", raw: []string{" a =1"}, err: true, strict: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := app.Context{
				Patches: tc.raw,
				Options: config.Options{Strict: tc.strict},
			}
			parsed, err := parsePatches(ctx)
			if tc.err {
				assert.Error(t, err, tc.name)
			} else {
				require.NoError(t, err, tc.name)
				for k, v := range tc.parsed {
					assert.Equal(t, v, parsed[k], "bad value for label %s", k)
				}
			}
		})
	}
}

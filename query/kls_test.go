// Copyright 2017-2020, Square, Inc.

package query_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre/query"
)

func TestParseIn(t *testing.T) {
	// Basic "x in (<values>)"
	sel := "x in (1,2,3)"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "in",
			Values: []string{"1", "2", "3"},
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// "in(<values>)": no space between "in" and value list
	sel = "x in(1,2,3)"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "in",
			Values: []string{"1", "2", "3"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)
}

func TestParseNotIn(t *testing.T) {
	// Basic "x notin (<values>)"
	sel := "x notin (1,2,3)"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "notin",
			Values: []string{"1", "2", "3"},
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// "notin(<values>)": no space between "notin" and value list
	sel = "x notin(1,2,3)"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "notin",
			Values: []string{"1", "2", "3"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

}

func TestParseEqual(t *testing.T) {
	// Basic "x = 1"
	sel := "x = 1"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"1"},
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// "x=1": no spacing
	sel = "x=1"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"1"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// Basic "x == 1"
	sel = "x == 1"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "==",
			Values: []string{"1"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// "x==1": no spacing
	sel = "x==1"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "==",
			Values: []string{"1"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

}

func TestParseNotEqual(t *testing.T) {
	// Basic "x != 1"
	sel := "x != 1"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "!=",
			Values: []string{"1"},
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// "x!=1": no spacing
	sel = "x!=1"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "!=",
			Values: []string{"1"},
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)
}

func TestParseInequality(t *testing.T) {
	ops := []string{"<", "<=", ">", ">="}
	for _, op := range ops {
		// With space
		sel := "x " + op + " 1"
		got, err := query.Parse(sel)
		require.NoError(t, err)

		expect := []query.Requirement{
			{
				Label:  "x",
				Op:     op,
				Values: []string{"1"},
			},
		}
		diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
		assert.Nil(t, diff)

		// No space
		sel = "x" + op + "1"
		got, err = query.Parse(sel)
		require.NoError(t, err)

		diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
		assert.Nil(t, diff)
	}
}

func TestParseMixed(t *testing.T) {

	// equality, exists
	sel := "x = y, z"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"y"},
		},
		{
			Label:  "z",
			Op:     "exists",
			Values: nil,
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// exists, exists, exists
	sel = "x,y,z"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "exists",
			Values: nil,
		},
		{
			Label:  "y",
			Op:     "exists",
			Values: nil,
		},
		{
			Label:  "z",
			Op:     "exists",
			Values: nil,
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// Everything
	sel = "x in (1,2), y notin(stage), z = foo, foo!=bar, app == shift, p, !p"
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{
		{
			Label:  "x",
			Op:     "in",
			Values: []string{"1", "2"},
		},
		{
			Label:  "y",
			Op:     "notin",
			Values: []string{"stage"},
		},
		{
			Label:  "z",
			Op:     "=",
			Values: []string{"foo"},
		},
		{
			Label:  "foo",
			Op:     "!=",
			Values: []string{"bar"},
		},
		{
			Label:  "app",
			Op:     "==",
			Values: []string{"shift"},
		},
		{
			Label:  "p",
			Op:     "exists",
			Values: nil,
		},
		{
			Label:  "p",
			Op:     "notexists",
			Values: nil,
		},
	}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)
}

func TestParseExcessiveSpacing(t *testing.T) {
	// Ignore spacing around everything
	sel := "  x =    y  , z      "
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"y"},
		},
		{
			Label:  "z",
			Op:     "exists",
			Values: nil,
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)

	// Nothing but space is an error. It could mean the query wasn't
	// auto-generated properly?
	sel = "                      "
	got, err = query.Parse(sel)
	require.Error(t, err)
	assert.Nil(t, got)

	// An empty string is not an error. It could imply "everything", i.e.
	// no requirements.
	sel = ""
	got, err = query.Parse(sel)
	require.NoError(t, err)
	expect = []query.Requirement{}
	diff = deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)
}

func TestParseQueryId(t *testing.T) {
	sel := "_id = 507f191e810c19729de860ea"
	got, err := query.Parse(sel)
	require.NoError(t, err)
	expect := []query.Requirement{
		{
			Label:  "_id",
			Op:     "=",
			Values: []string{"507f191e810c19729de860ea"},
		},
	}
	diff := deep.Equal(got, expect) // can't use assert.Equal because some unexported fields don't match. deep.Equal only compares exported fields.
	assert.Nil(t, diff)
}

func TestParseInvalid(t *testing.T) {
	invalid := []string{
		// Invalid first chars
		"=val",
		"(label)=val",
		")label=val",
		"=bar=val",
		"<bar=val",
		">label=val",
		"%label=val",
		"&label=val",
		"?label=val",
		"*label=val",
		"^label=val",
		"+label=val",
		"~label=val",
		"!!label=val",
		`\label=val`,

		// Invalid inner chars
		"label name=val",
		//"label<name=val", // ambiguous, cannot reliably parse
		//"label>name=val", // ambiguous, cannot reliably parse
		"label%name=val",
		"label&name=val",
		"label?name=val",
		"label*name=val",
		"label^name=val",
		"label+name=val",
		"label~name=val",
		"label!name",
		`label\name=val`,
	}
	for _, sel := range invalid {
		got, err := query.Parse(sel)
		assert.Error(t, err, "selector '%s' is invalid but did not cause an error: %s", sel, got)
	}
}

func TestParseValidLabels(t *testing.T) {
	// Not that all of these are good label names, but they're allowed nonetheless
	invalid := []string{
		"@user=foo",
		"#channel=foo",
		"$cashTag=foo",
		"/tmp=foo",
		"/tmp/dir=foo",
		"_tmp=foo",
		"_tmp_dir=foo",
		"-option=foo",
		"--option=foo",
		"label-name=foo",
		"label.name=foo",
		"user@email.com=foo",
		"pkg@v1.0.0=foo",
		"https://local.host=foo",
	}
	for _, sel := range invalid {
		got, err := query.Parse(sel)
		require.NoError(t, err, "selector '%s' is valid but caused an error: %s", sel, err)
		assert.True(t, len(got) == 1 && len(got[0].Values) == 1 && got[0].Values[0] == "foo", "selector '%s' parsed wrong value: %+v", sel, got)
	}
}

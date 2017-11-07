package kls_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/square/etre/kls"
)

func TestIn(t *testing.T) {
	// Basic "x in (<values>)"
	sel := "x in (1,2,3)"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
		{
			Label:  "x",
			Op:     "in",
			Values: []string{"1", "2", "3"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// "in(<values>)": no space between "in" and value list
	sel = "x in(1,2,3)"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "in",
			Values: []string{"1", "2", "3"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestNotIn(t *testing.T) {
	// Basic "x notin (<values>)"
	sel := "x notin (1,2,3)"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
		{
			Label:  "x",
			Op:     "notin",
			Values: []string{"1", "2", "3"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// "notin(<values>)": no space between "notin" and value list
	sel = "x notin(1,2,3)"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "notin",
			Values: []string{"1", "2", "3"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestEqual(t *testing.T) {
	// Basic "x = 1"
	sel := "x = 1"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// "x=1": no spacing
	sel = "x=1"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "=",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// Basic "x == 1"
	sel = "x == 1"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "==",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// "x==1": no spacing
	sel = "x==1"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "==",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestNotEqual(t *testing.T) {
	// Basic "x != 1"
	sel := "x != 1"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
		{
			Label:  "x",
			Op:     "!=",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// "x!=1": no spacing
	sel = "x!=1"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
		{
			Label:  "x",
			Op:     "!=",
			Values: []string{"1"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestInequality(t *testing.T) {
	ops := []string{"<", "<=", ">", ">="}
	for _, op := range ops {
		// With space
		sel := "x " + op + " 1"
		got, err := kls.Parse(sel)
		if err != nil {
			t.Fatal(err)
		}
		expect := []kls.Requirement{
			{
				Label:  "x",
				Op:     op,
				Values: []string{"1"},
			},
		}
		if diff := deep.Equal(got, expect); diff != nil {
			t.Error(diff)
		}

		// No space
		sel = "x" + op + "1"
		got, err = kls.Parse(sel)
		if err != nil {
			t.Fatal(err)
		}
		if diff := deep.Equal(got, expect); diff != nil {
			t.Error(diff)
		}
	}
}

func TestMixed(t *testing.T) {

	// equality, exists
	sel := "x = y, z"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
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
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// exists, exists, exists
	sel = "x,y,z"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
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
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// Everything
	sel = "x in (1,2), y notin(stage), z = foo, foo!=bar, app == shift, p, !p"
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{
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
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestExcessiveSpacing(t *testing.T) {

	// Ignore spacing around everything
	sel := "  x =    y  , z      "
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
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
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}

	// Nothing but space is an error. It could mean the query wasn't
	// auto-generated properly?
	sel = "                      "
	got, err = kls.Parse(sel)
	if err == nil {
		t.Errorf("err is nil, expected an error")
	}
	if got != nil {
		t.Errorf("got %+v, expected nil []Requirement", got)
	}

	// An empty string is not an error. It could imply "everything", i.e.
	// no requirements.
	sel = ""
	got, err = kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect = []kls.Requirement{}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestQueryId(t *testing.T) {
	sel := "_id = 507f191e810c19729de860ea"
	got, err := kls.Parse(sel)
	if err != nil {
		t.Fatal(err)
	}
	expect := []kls.Requirement{
		{
			Label:  "_id",
			Op:     "=",
			Values: []string{"507f191e810c19729de860ea"},
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

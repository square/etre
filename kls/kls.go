// Copyright 2017, Square, Inc.

// Package kls provides a light-weight parser for Kubernetes label selectors.
package kls

import (
	"fmt"
	"strings"
)

// A Requirement represents one predicate parsed from a selector. For example,
// selector "x=y,z" has two requirements: "x=y" and "z". Op is the literal operator,
// or "exists" (p) or "notexists" (!p).
type Requirement struct {
	Label  string
	Op     string
	Values []string
	val    string // raw value
}

const (
	state_space byte = iota
	state_label
	state_op
	state_eq_op
	state_set_op
	state_value
)

var stateName = map[byte]string{
	0: "space",
	1: "label",
	2: "op",
	3: "eq_op",
	4: "set_op",
	5: "value",
}

// All ops end with =. Technically, only = and == are equality, but
// we call them all "equality" for convenience.
var eqOp = map[rune]bool{
	'=': true, // =, ==
	'!': true, // !=
	'>': true, // >, >=
	'<': true, // <, <=
}

var Debug = false

func Parse(selector string) ([]Requirement, error) {
	if selector == "" {
		return []Requirement{}, nil
	}

	// Split selector into distinct predicates: x=y,z -> [x=y, z]
	// This makes parsing each predicate (below) a little simpler
	// because once we find the op, we can presume the rest of the
	// string is the value, if any.
	startOffset := 0
	pred := []string{}
	inValueList := false // skip commas inside "(val1,valN)"
	for endOffset, r := range selector {
		if inValueList {
			if r == ')' {
				inValueList = false
			}
			continue
		}
		if r == '(' {
			inValueList = true
			continue
		}
		if r != ',' {
			continue
		}
		pred = append(pred, selector[startOffset:endOffset])
		startOffset = endOffset + 1 // first char after ,
	}
	if startOffset < len(selector) {
		// Last predicate to end of selector, e.g. "bar" in "x=y,foo,bar"
		pred = append(pred, selector[startOffset:])
	}

	all := make([]Requirement, len(pred))
	reqNo := 0

	for n, selector := range pred {
		if Debug {
			fmt.Printf("parsing '%s' (%d)\n", selector, len(selector))
		}
		req := Requirement{}
		left := 0
		state := state_space
		next := state_label
	PARSE_LOOP:
		for right, cur := range selector {
			switch state {
			case state_space:
				if isSpace(cur) {
					continue
				}

				if Debug {
					fmt.Printf("state change 1: %s -> %s\n", stateName[state], stateName[next])
				}
				state = next // state change

				switch state {
				case state_label:
					if cur != '!' {
						if Debug {
							fmt.Printf("first char of label at %d\n", right)
						}
						left = right // 1st char of label
					} else {
						// Label begins with not-exists op: "!foo"
						req.Op = "notexists"
						if Debug {
							fmt.Printf("not exists (%d)\n", n)
							fmt.Printf("state change 3: %s -> %s\n", stateName[state], stateName[state_space])
						}
						state = state_space
						next = state_label
					}
				case state_op:
					left = right // 1st char of op
					if eqOp[cur] {
						state = state_eq_op
					} else {
						state = state_set_op
					}
					if Debug {
						fmt.Printf("%s\n", stateName[state])
					}
				case state_value:
					if Debug {
						fmt.Printf("value from '%s' at %d (1)\n", string(cur), right)
					}
					left = right
					break PARSE_LOOP
				}
			case state_label:
				// Label ends on op char or space
				if !isSpace(cur) && !eqOp[cur] {
					continue // more chars in label
				}
				req.Label = selector[left:right] // label ends
				if eqOp[cur] {
					// No space between label and op: "foo=bar"
					if req.Op != "" {
						return nil, fmt.Errorf("already have op: %s", req.Op)
					}
					if Debug {
						fmt.Printf("state change 2: %s -> %s\n", stateName[state], stateName[state_eq_op])
						fmt.Printf("first char of op at %d (2)\n", right)
					}
					state = state_eq_op // state change
					left = right        // 1st char of op
				} else {
					// Space between label and op: "foo = bar"
					if Debug {
						fmt.Printf("state change 3: %s -> %s\n", stateName[state], stateName[state_space])
					}
					state = state_space // state change
					next = state_op
				}
			case state_set_op, state_eq_op:
				switch state {
				case state_set_op:
					// Set op ends on ( or space
					if !isSpace(cur) && cur != '(' {
						continue // more chars in op
					}
				case state_eq_op:
					// Set op ends on space or non-op char
					if !isSpace(cur) && cur == '=' {
						continue // more chars in op
					}
				}
				req.Op = selector[left:right] // op ends
				if !isSpace(cur) {
					if Debug {
						fmt.Printf("value from '%s' at %d (2)\n", string(cur), right)
					}
					if cur == '(' && (req.Op != "in" && req.Op != "notin") {
						return nil, fmt.Errorf("not not( and notin(")
					}
					left = right
					state = state_value // state change
					break PARSE_LOOP
				} else {
					// Space between op and value list: "in (<values>)"
					if Debug {
						fmt.Printf("state change 4: %s -> %s\n", stateName[state], stateName[state_space])
					}
					state = state_space // state change
					next = state_value
				}
			}
		}

		switch state {
		case state_label:
			req.Label = selector[left:]
			if req.Op == "" {
				req.Op = "exists"
			}
		case state_op, state_eq_op, state_set_op:
			return nil, fmt.Errorf("stopped parsing in %s", stateName[state])
		case state_value:
			if req.Label == "" || req.Op == "" {
				return nil, fmt.Errorf("stopped parsing in state_value")
			}
			req.val = strings.TrimSpace(selector[left:])
		case state_space:
			if req.Op != "" {
				return nil, fmt.Errorf("no value after op")
			} else if req.Label != "" {
				req.Op = "exists"
			} else {
				return nil, fmt.Errorf("empty string")
			}
		default:
			return nil, fmt.Errorf("stopped parsing in %s", stateName[state])
		}

		if eqOp[rune(req.Op[0])] {
			req.Values = []string{req.val}
		} else if req.Op == "in" || req.Op == "notin" {
			if len(req.val) < 3 {
				return nil, fmt.Errorf("invalid [not]in value list: %s", req.val)
			}
			req.Values = strings.Split(req.val[1:len(req.val)-1], ",")
		} else if req.Op == "exists" || req.Op == "notexists" {
			req.Values = nil
		} else {
			return nil, fmt.Errorf("invalid op: %s", req.Op)
		}

		if Debug {
			fmt.Printf("REQ: %+v\n", req)
		}
		all[reqNo] = req
		reqNo++
	}

	return all, nil
}

func isSpace(r rune) bool {
	return r == 0x20 || r == 0x09 || r == 0x0D || r == 0x0A
}

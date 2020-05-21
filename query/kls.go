// Copyright 2017-2020, Square, Inc.

package query

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
	state_op        // op -> state_symbol_op || state_set_op
	state_symbol_op // =, !, <, >
	state_set_op    // in, notin
	state_value
)

var stateName = map[byte]string{
	0: "space",
	1: "label",
	2: "op",
	3: "symbol_op",
	4: "set_op",
	5: "value",
}

// IsOp returns true if the rune is an operator character.
func IsOp(r rune) bool {
	// This used to be a map[run]bool, but this func is 10x faster:
	//   BenchmarkHash-8   	166499444	         7.23 ns/op
	//   BenchmarkFunc-8   	1000000000	         0.502 ns/op
	return r == '=' || r == '!' || r == '>' || r == '<'
}

// IsInvalidLabelChar returns true if the character is invalid for a label.
func IsInvalidLabelChar(r rune) bool {
	// Implicitly want these:
	//   @username
	//   #channel
	//   $cashTag
	//   -flag
	//   _metalabel
	//   /directory
	//
	// Explicitly do not want these:
	//
	return IsOp(r) || // cannot be operator
		r == '%' || // HTML escape: "%20"
		r == '&' || // HTML param: "?a=b&foo=bar"
		r == '?' || // HTML query: "?foo=bar"
		r == '(' || // avoid confusion with [not]in()
		r == ')' || // avoid confusion with [not]in()
		r == '^' || // reserved for OR operator
		r == '+' || // reserved for addition (es --update entity cnt+=1)
		r == '~' || // reserved for pattern match (es entity cnt=~foo)
		r == '\\' || // escape char
		r == '*' // reserved for wildcard
}

var Debug = false

// Parse parses	a Kubernetes Label Selector sttring.
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
						if IsInvalidLabelChar(cur) {
							return nil, fmt.Errorf("'%s': invalid label first character: %s", selector, string(cur))
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
					if IsOp(cur) {
						state = state_symbol_op
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
				// Label char if not space or operator
				if !isSpace(cur) && !IsOp(cur) {
					if IsInvalidLabelChar(cur) {
						return nil, fmt.Errorf("%s: invalid label character: %s", selector, string(cur))
					}
					continue // more label chars
				}
				req.Label = selector[left:right] // label ends
				if IsOp(cur) {
					// No space between label and op: "foo=bar"
					if req.Op != "" {
						return nil, fmt.Errorf("already have op: %s", req.Op)
					}
					if Debug {
						fmt.Printf("state change 2: %s -> %s\n", stateName[state], stateName[state_symbol_op])
						fmt.Printf("first char of op at %d (2)\n", right)
					}
					state = state_symbol_op // state change
					left = right            // 1st char of op
				} else {
					// Space between label and op: "foo = bar"
					if Debug {
						fmt.Printf("state change 3: %s -> %s\n", stateName[state], stateName[state_space])
					}
					state = state_space // state change
					next = state_op
				}
			case state_set_op, state_symbol_op:
				switch state {
				case state_set_op:
					// Set op ends on ( or space
					if !isSpace(cur) && cur != '(' {
						continue // more chars in op
					}
				case state_symbol_op:
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
						return nil, fmt.Errorf("'(' is not valid after '%s' operator, only valid after 'not' or 'notin' operator", string(cur))
					}
					if req.Op == "!" {
						return nil, fmt.Errorf("%s: invalid not-equal operator: missing '=' after '!'", selector)
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
		case state_op, state_symbol_op, state_set_op:
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

		if IsOp(rune(req.Op[0])) {
			req.Values = []string{req.val}
		} else if req.Op == "in" || req.Op == "notin" {
			if len(req.val) < 3 {
				return nil, fmt.Errorf("invalid [not]in value list: %s", req.val)
			}
			req.Values = strings.Split(req.val[1:len(req.val)-1], ",")
		} else if req.Op == "exists" || req.Op == "notexists" {
			// No values
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

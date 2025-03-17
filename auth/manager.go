// Copyright 2018-2019, Square, Inc.

package auth

import (
	"fmt"
	"net/http"
	"strings"
)

type Manager struct {
	disabled bool
	plugin   Plugin
	acl      map[string]ACL
}

func NewManager(acls []ACL, plugin Plugin) Manager {
	byRole := map[string]ACL{}
	for _, acl := range acls {
		byRole[acl.Role] = acl
	}
	return Manager{
		disabled: len(acls) == 0, // no auth if no acls
		plugin:   plugin,
		acl:      byRole,
	}
}

func (m Manager) Authenticate(req *http.Request) (Caller, error) {
	caller, err := m.plugin.Authenticate(req)
	if err != nil {
		return caller, err
	}

	// Set any key-value trace values from header if not already set by plugin
	traceValue := req.Header.Get("X-Etre-Trace")
	if traceValue != "" {
		if caller.Trace == nil {
			caller.Trace = map[string]string{}
		}
		keyValPairs := strings.Split(traceValue, ",")
		for _, kv := range keyValPairs {
			p := strings.SplitN(kv, "=", 2)
			if len(p) != 2 {
				continue // bad value, ignore
			}
			// Set trace value if not already set by plugin,
			// i.e. values from plugin takes precedence
			if _, ok := caller.Trace[p[0]]; ok {
				continue // already set by plugin, ignore
			}
			caller.Trace[p[0]] = p[1]
		}
	}

	// No ACLs = no auth
	if m.disabled {
		return caller, nil
	}
	for _, role := range caller.Roles {
		acl, ok := m.acl[role]
		if !ok {
			continue
		}
		if len(acl.TraceKeysRequired) > 0 {
			if caller.Trace == nil {
				return caller, fmt.Errorf("role %s requires trace keys %v but caller has no trace data", role, acl.TraceKeysRequired)
			}
			for _, requiredKey := range acl.TraceKeysRequired {
				if _, ok := caller.Trace[requiredKey]; !ok {
					return caller, fmt.Errorf("role %s requires trace key %s but caller does not have it; caller trace: %+v", role, requiredKey, caller.Trace)
				}
			}
		}
	}
	return caller, nil
}

func (m Manager) Authorize(caller Caller, a Action) error {
	// No ACLs = no auth
	if m.disabled {
		return m.plugin.Authorize(caller, a)
	}

	// Check each caller role against configured role ACLs
	allowed := false
	opName := ""

	// Check all roles until we find one that allows access
	for i := 0; i < len(caller.Roles) && !allowed; i++ {
		acl := m.acl[caller.Roles[i]]

		switch a.Op {
		case OP_READ:
			opName = "reading"
			allowed = acl.Admin || inList(a.EntityType, acl.Read)
		case OP_WRITE:
			opName = "writing"
			allowed = acl.Admin || inList(a.EntityType, acl.Write)
		case OP_CDC:
			opName = "CDC"
			allowed = acl.Admin || acl.CDC
		}
	}
	if !allowed {
		return fmt.Errorf("caller %s has no role that allows %s %s entities; caller roles: %v", caller.Name, opName, a.EntityType, caller.Roles)
	}

	// Let plugin do final authorization
	return m.plugin.Authorize(caller, a)
}

func inList(s string, l []string) bool {
	for _, v := range l {
		if s == v {
			return true
		}
	}
	return false
}

package database

import "golang.org/x/exp/maps"

type set map[string]struct{}

type transaction struct {
	delNames     set
	nameToValue  map[string]string
	valueToNames map[string]set
	prev         *transaction
}

func (t *transaction) get(name string) string {
	if _, ok := t.delNames[name]; ok {
		return nullValue
	}
	if updateVal, ok := t.nameToValue[name]; ok {
		return updateVal
	}
	if t.prev != nil {
		return t.prev.get(name)
	}
	return nullValue
}

func (t *transaction) set(name, value string) {
	if t.prev != nil && t.prev.get(name) == value {
		return
	}
	delete(t.delNames, name)
	t.nameToValue[name] = value
	if names, ok := t.valueToNames[value]; ok {
		names[name] = struct{}{}
		t.valueToNames[value] = names
	} else {
		t.valueToNames[value] = set{
			name: {},
		}
	}
}

func (t *transaction) delete(name string) {
	t.delNames[name] = struct{}{}
	delete(t.nameToValue, name)
	val, ok := t.nameToValue[name]
	if !ok {
		return
	}
	names, ok := t.valueToNames[val]
	if !ok {
		return
	}
	for k := range names {
		if k == name {
			delete(names, k)
		}
	}
}

func (t *transaction) count(value string) int {
	return len(t.names(value))
}

func (t *transaction) names(value string) set {
	allNames := make(set)
	if t.prev != nil {
		allNames = t.prev.names(value)
	}
	for name := range allNames {
		if _, ok := t.nameToValue[name]; ok {
			delete(allNames, name)
		}
		if _, ok := t.delNames[name]; ok {
			delete(allNames, name)
		}
	}
	if names, ok := t.valueToNames[value]; ok {
		maps.Copy(allNames, names)
	}
	return allNames
}

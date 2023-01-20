package database

import (
	"errors"
	"golang.org/x/exp/maps"
)

var (
	TransactionNotFoundErr = errors.New("TRANSACTION NOT FOUND")
)

type transaction struct {
	db                 *Database
	delNames           set
	updateNameToValue  map[string]string
	updateValueToNames map[string]set
	prev               *transaction
}

func (t *transaction) beginTransaction() *transaction {
	return &transaction{
		delNames:           make(set),
		updateNameToValue:  make(map[string]string),
		updateValueToNames: make(map[string]set),
		prev:               t,
	}

}

func (t *transaction) rollback() (*transaction, error) {
	if t.prev == nil {
		return nil, TransactionNotFoundErr
	}
	return t.prev, nil
}

func (t *transaction) commit() *transaction {
	if t.db != nil {
		return t
	}
	for name, value := range t.updateNameToValue {
		t.prev.set(name, value)
	}
	for name := range t.delNames {
		t.prev.delete(name)
	}
	return t.prev.commit()
}

func (t *transaction) get(name string) string {
	if t.db != nil {
		val, ok := t.db.nameToValue[name]
		if !ok {
			return nullValue
		}
		return val
	}
	if _, ok := t.delNames[name]; ok {
		return nullValue
	}
	if updateVal, ok := t.updateNameToValue[name]; ok {
		return updateVal
	}
	if t.prev != nil {
		return t.prev.get(name)
	}
	return nullValue
}

func (t *transaction) set(name, value string) {
	if t.db != nil {
		t.db.nameToValue[name] = value
		names := t.db.valueToNames[value]
		names[name] = struct{}{}
		t.db.valueToNames[value] = names
		return
	}
	if t.prev != nil && t.prev.get(name) == value {
		return
	}
	delete(t.delNames, name)
	t.updateNameToValue[name] = value
	if names, ok := t.updateValueToNames[value]; ok {
		names[name] = struct{}{}
		t.updateValueToNames[value] = names
	} else {
		t.updateValueToNames[value] = set{
			name: {},
		}
	}
}

func (t *transaction) delete(name string) {
	if t.db != nil {
		doDelete(name, t.db.nameToValue, t.db.valueToNames)
		return
	}
	delete(t.delNames, name)
	doDelete(name, t.updateNameToValue, t.updateValueToNames)
}

func doDelete(name string, nameToValue map[string]string, valueToNames map[string]set) {
	val, ok := nameToValue[name]
	if !ok {
		return
	}
	delete(nameToValue, name)
	names, ok := valueToNames[val]
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
	if t.db != nil {
		return len(t.db.valueToNames[value])
	}
	return len(t.names(value))
}

func (t *transaction) names(value string) set {
	allNames := make(set)
	if t.prev != nil {
		allNames = t.prev.names(value)
	}
	for name := range allNames {
		if _, ok := t.updateNameToValue[name]; ok {
			delete(allNames, name)
		}
		if _, ok := t.delNames[name]; ok {
			delete(allNames, name)
		}
	}
	if names, ok := t.updateValueToNames[value]; ok {
		maps.Copy(allNames, names)
	}
	return allNames
}

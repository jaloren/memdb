package database

import (
	"errors"
	"golang.org/x/exp/maps"
)

var (
	TransactionNotFoundErr = errors.New("TRANSACTION NOT FOUND")
)

type Txn struct {
	db                 *Database
	delNames           set
	updateNameToValue  map[string]string
	updateValueToNames map[string]set
	prev               *Txn
}

func (t *Txn) Begin() *Txn {
	return &Txn{
		delNames:           make(set),
		updateNameToValue:  make(map[string]string),
		updateValueToNames: make(map[string]set),
		prev:               t,
	}
}

func (t *Txn) Rollback() (*Txn, error) {
	if t.prev == nil {
		return nil, TransactionNotFoundErr
	}
	return t.prev, nil
}

func (t *Txn) Commit() *Txn {
	if t.db != nil {
		return t
	}
	for name, value := range t.updateNameToValue {
		t.prev.Set(name, value)
	}
	for name := range t.delNames {
		t.prev.Delete(name)
	}
	return t.prev.Commit()
}

func (t *Txn) Get(name string) string {
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
		return t.prev.Get(name)
	}
	return nullValue
}

func (t *Txn) Set(name, value string) {
	if t.db != nil {
		t.db.nameToValue[name] = value
		names, ok := t.db.valueToNames[value]
		if ok {
			names[name] = struct{}{}
			t.db.valueToNames[value] = names
		} else {
			t.db.valueToNames[value] = set{
				name: struct{}{},
			}
		}
		return
	}
	if t.prev != nil && t.prev.Get(name) == value {
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

func (t *Txn) Delete(name string) {
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

func (t *Txn) Count(value string) int {
	if t.db != nil {
		return len(t.db.valueToNames[value])
	}
	return len(t.names(value))
}

func (t *Txn) names(value string) set {
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

package database

import (
	"errors"
	"golang.org/x/exp/maps"
)

const (
	NullValue = "NULL"
)

var (
	EndOpErr               = errors.New("exiting database")
	TransactionNotFoundErr = errors.New("TRANSACTION NOT FOUND")
)

type set map[string]struct{}

type Database struct {
	nameToValue  map[string]string
	valueToNames map[string]set
	Transaction  *Txn
}

func New() *Database {
	db := &Database{
		nameToValue:  make(map[string]string),
		valueToNames: make(map[string]set),
	}
	db.Transaction = &Txn{
		delNames:           make(set),
		updateNameToValue:  make(map[string]string),
		updateValueToNames: make(map[string]set),
		db:                 db,
	}
	return db
}

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
			return NullValue
		}
		return val
	}
	if _, ok := t.delNames[name]; ok {
		return NullValue
	}
	if updateVal, ok := t.updateNameToValue[name]; ok {
		return updateVal
	}
	if t.prev != nil {
		return t.prev.Get(name)
	}
	return NullValue
}

func (t *Txn) Set(name, value string) {
	if t.db != nil {
		exist, ok := t.db.nameToValue[name]
		existingNames := t.db.valueToNames[exist]
		delete(existingNames, name)
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
	t.delNames[name] = struct{}{}
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
	if t.db != nil {
		if dbNames, ok := t.db.valueToNames[value]; ok {
			maps.Copy(allNames, dbNames)
		}
	}
	return allNames
}

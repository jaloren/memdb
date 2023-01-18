package database

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const (
	nullValue  = "NULL"
	beginOp    = "BEGIN"
	commitOp   = "COMMIT"
	delOp      = "DELETE"
	getOp      = "GET"
	endOp      = "END"
	rollbackOp = "ROLLBACK"
	countOp    = "COUNT"
	setOp      = "SET"
)

var (
	TransactionNotFoundErr = errors.New("TRANSACTION NOT FOUND")
	EndOpErr               = errors.New("exiting database")
	supportedOps           = []string{
		beginOp, commitOp, delOp, getOp, endOp, rollbackOp, setOp,
	}
)

type Database struct {
	data         map[string]*string
	valCnt       map[string]int
	mu           sync.RWMutex
	transactions *transaction
}

func New() *Database {
	return &Database{
		data:   make(map[string]*string),
		valCnt: make(map[string]int),
		mu:     sync.RWMutex{},
	}
}

func (d *Database) Set(name, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.transactions != nil {
		d.transactions.add(d, name, ptr(value))
		return
	}
	if existing, ok := d.data[name]; ok && existing != nil {
		// there's an existing name, but the value is being updated so decrement the old value counter
		// and increment the new value counter
		d.decreValCnt(deref(existing))
	} else if ok && deref(existing) == value {
		// to avoid double counting duplicate puts return early
		return
	}
	d.increValCnt(value)
	d.data[name] = ptr(value)
}

func (d *Database) Get(name string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.transactions != nil {
		currentTransaction := d.transactions
		val, ok := currentTransaction.get(name)
		if ok {
			return val
		}
	}
	val, ok := d.data[name]
	if !ok || val == nil {
		return nullValue
	}
	return deref(val)
}

func (d *Database) Delete(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.transactions != nil {
		d.transactions.add(d, name, nil)
		return
	}
	if existing, ok := d.data[name]; ok && existing == nil {
		// to avoid double counting duplicate delete return early
		return
	} else if ok {
		d.decreValCnt(deref(existing))
	}
	d.data[name] = nil
}

func (d *Database) Count(value string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.transactions == nil {
		totalCnt, _ := d.valCnt[value]
		return totalCnt
	}
	currentTrans := d.transactions
	for {
		for transVal, transValCnt := range currentTrans.valCnt {
			if transVal != value {
				continue
			}
			return transValCnt
		}
		if currentTrans.prev == nil {
			break
		}
		currentTrans = currentTrans.prev
	}
	return 0
}

func (d *Database) BeginTransaction() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.transactions == nil {
		d.transactions = newTransaction(nil)
	} else {
		d.transactions = newTransaction(d.transactions)
	}
}

func (d *Database) CommitTransactions() {
	d.mu.Lock()
	defer d.mu.Unlock()
	currentTrans := d.transactions
	for {
		for name, value := range currentTrans.data {
			d.data[name] = value
			for valNames, cnt := range currentTrans.valCnt {
				if origCnt, ok := d.valCnt[valNames]; ok {
					d.valCnt[valNames] = origCnt + cnt
				} else {
					d.valCnt[valNames] = cnt
				}
			}
		}
		if currentTrans.prev == nil {
			break
		}
		currentTrans = currentTrans.prev
	}
	d.transactions = nil
}

func (d *Database) RollbackLastTransaction() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.transactions == nil {
		return TransactionNotFoundErr
	}
	if d.transactions.prev == nil {
		d.transactions = nil
		return nil
	}
	d.transactions = d.transactions.prev
	return nil
}

func (d *Database) increValCnt(value string) {
	cnt, ok := d.valCnt[value]
	if ok {
		d.valCnt[value] = cnt + 1
	} else {
		d.valCnt[value] = 1
	}
}

func (d *Database) decreValCnt(value string) {
	cnt, ok := d.valCnt[value]
	if ok && cnt > 0 {
		d.valCnt[value] = cnt - 1
	}
}

func (d *Database) ExecStatement(text string) (string, error) {
	parts := strings.Split(text, " ")
	operation := parts[0]
	if operation == "" {
		return "", fmt.Errorf("database operation is not specified: statement must start with one of the following: " +
			strings.Join(supportedOps, " "))
	}
	switch operation {
	case delOp:
		return "", d.parseDelStmt(parts)
	case getOp:
		return d.parseGetStmt(parts)
	case setOp:
		return "", d.parseSetStmt(parts)
	case beginOp:
		d.BeginTransaction()
		return "", nil
	case endOp:
		return "", EndOpErr
	case commitOp:
		d.CommitTransactions()
		return "", nil
	case countOp:
		return d.parseCntStmt(parts)
	case rollbackOp:
		return "", d.RollbackLastTransaction()
	default:
		return "", fmt.Errorf("database does not support operation %s: supported operations: %s",
			operation, strings.Join(supportedOps, " "))
	}
}

func (d *Database) mustExecStatement(text string) string {
	result, err := d.ExecStatement(text)
	if err != nil {
		panic(err)
	}
	return result
}

func (d *Database) parseCntStmt(parts []string) (string, error) {
	if len(parts) < 2 || parts[1] == "" {
		return "", fmt.Errorf("failed to execute operation %s: statement %q does not have a value",
			countOp, strings.Join(parts, " "))
	}
	value := parts[1]
	return strconv.Itoa(d.Count(value)), nil
}

func (d *Database) parseDelStmt(parts []string) error {
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("failed to execute operation %s: statement %q does not have a name",
			delOp, strings.Join(parts, " "))
	}
	name := parts[1]
	d.Delete(name)
	return nil
}

func (d *Database) parseGetStmt(parts []string) (string, error) {
	if len(parts) < 2 || parts[1] == "" {
		return "", fmt.Errorf("failed to execute operation %s: statement %q does not have a name",
			getOp, strings.Join(parts, " "))
	}
	name := parts[1]
	return d.Get(name), nil
}

func (d *Database) parseSetStmt(parts []string) error {
	if len(parts) < 3 || (parts[1] == "" || parts[2] == "") {
		return fmt.Errorf("failed to execute operation %s: statement %q does not have a name or valie`",
			setOp, strings.Join(parts, " "))
	}
	name := parts[1]
	val := parts[2]
	d.Set(name, val)
	return nil
}

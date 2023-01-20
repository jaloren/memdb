package database

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	EndOpErr     = errors.New("exiting database")
	supportedOps = []string{
		beginOp, commitOp, delOp, getOp, endOp, rollbackOp, setOp,
	}
)

type set map[string]struct{}

type Database struct {
	nameToValue  map[string]string
	valueToNames map[string]set
	trans        *transaction
}

func New() *Database {
	db := &Database{
		nameToValue:  make(map[string]string),
		valueToNames: make(map[string]set),
		trans:        nil,
	}
	db.trans = &transaction{
		delNames:           make(set),
		updateNameToValue:  make(map[string]string),
		updateValueToNames: make(map[string]set),
		db:                 db,
	}
	return db
}

func (d *Database) Set(name, value string) {
}

func (d *Database) Get(name string) string {
}

func (d *Database) Delete(name string) {
}

func (d *Database) Count(value string) int {
}

func (d *Database) BeginTransaction() {
}

func (d *Database) CommitTransactions() {
}

func (d *Database) RollbackLastTransaction() error {
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

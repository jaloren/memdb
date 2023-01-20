package database

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestExampleOne(t *testing.T) {
	claim := assert.New(t)
	db := New()
	claim.Equal(nullValue, db.mustExecStatement(`GET a`))
	db.mustExecStatement(`SET a foo`)        // 1
	db.mustExecStatement(`SET b foo`)        // 2
	claim.NoError(assertCount(db, "foo", 2)) // 2
	claim.NoError(assertCount(db, "bar", 0)) // 2
	db.mustExecStatement(`DELETE a`)         // 1
	claim.NoError(assertCount(db, "foo", 1))
	db.mustExecStatement("SET b baz")
	claim.NoError(assertCount(db, "foo", 0))
	claim.Equal("baz", db.mustExecStatement("GET b"))
	claim.Equal(nullValue, db.mustExecStatement("GET B"))
}

func TestExampleTwo(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.mustExecStatement("SET a foo")
	db.mustExecStatement("SET a foo")
	claim.NoError(assertCount(db, "foo", 1))
	claim.Equal("foo", db.mustExecStatement("GET a"))
	db.mustExecStatement("DELETE a")
	claim.Equal(nullValue, db.mustExecStatement("GET a"))
	claim.NoError(assertCount(db, "foo", 0))
}

func TestExampleThree(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.mustExecStatement("BEGIN")
	db.mustExecStatement("SET a foo")
	claim.Equal("foo", db.mustExecStatement("GET a"))
	db.mustExecStatement("BEGIN")
	db.mustExecStatement("SET a bar")
	claim.Equal("bar", db.mustExecStatement("GET a"))
	db.mustExecStatement("SET a baz")
	db.mustExecStatement("ROLLBACK")
	claim.Equal("foo", db.mustExecStatement("GET a"))
	db.mustExecStatement("ROLLBACK")
	claim.Equal(nullValue, db.mustExecStatement("GET a"))
}

func TestExampleFour(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.mustExecStatement("SET a foo")
	db.mustExecStatement("SET b baz")
	db.mustExecStatement("BEGIN")
	claim.Equal("foo", db.mustExecStatement("GET a"))
	db.mustExecStatement("SET a bar")
	claim.NoError(assertCount(db, "bar", 1))
	db.mustExecStatement("BEGIN")
	claim.NoError(assertCount(db, "bar", 1))
	db.mustExecStatement("DELETE a")
	claim.Equal(nullValue, db.mustExecStatement("GET a"))
	claim.NoError(assertCount(db, "bar", 0))
	db.mustExecStatement("ROLLBACK")
	claim.Equal("bar", db.mustExecStatement("GET a"))
	claim.NoError(assertCount(db, "bar", 1))
	db.mustExecStatement("COMMIT")
	claim.Equal("bar", db.mustExecStatement("GET a"))
	claim.Equal("baz", db.mustExecStatement("GET b"))
}

func assertCount(db *Database, value string, expected int) error {
	result, err := db.ExecStatement(`COUNT ` + value)
	if err != nil {
		return err
	}
	actual, err := strconv.Atoi(result)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("for value %s, expected Count: %d, actual Count: %d", value, expected, actual)
	}
	return nil
}

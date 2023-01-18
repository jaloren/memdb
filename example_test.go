package main

import (
	"fmt"
	"github.com/jaloren/memdb/database"
	"github.com/jaloren/memdb/parser"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestExampleOne(t *testing.T) {
	claim := assert.New(t)
	db := database.New()
	claim.Equal(database.NullValue, mustGet(db, "a"))
	mustExecStatement(db, `SET a foo`)       // 1
	mustExecStatement(db, `SET b foo`)       // 2
	claim.NoError(assertCount(db, "foo", 2)) // 2
	claim.NoError(assertCount(db, "bar", 0)) // 2
	mustExecStatement(db, `DELETE a`)        // 1
	claim.NoError(assertCount(db, "foo", 1))
	mustExecStatement(db, "SET b baz")
	claim.NoError(assertCount(db, "foo", 0))
	claim.Equal("baz", mustGet(db, "b"))
	claim.Equal(database.NullValue, mustGet(db, "B"))
}

func TestExampleTwo(t *testing.T) {
	claim := assert.New(t)
	db := database.New()
	mustExecStatement(db, "SET a foo")
	mustExecStatement(db, "SET a foo")
	claim.NoError(assertCount(db, "foo", 1))
	claim.Equal("foo", mustGet(db, "a"))
	mustExecStatement(db, "DELETE a")
	claim.Equal(database.NullValue, mustGet(db, "a"))
	claim.NoError(assertCount(db, "foo", 0))
}

func TestExampleThree(t *testing.T) {
	claim := assert.New(t)
	db := database.New()
	mustExecStatement(db, "BEGIN")
	mustExecStatement(db, "SET a foo")
	claim.Equal("foo", mustGet(db, "a"))
	mustExecStatement(db, "BEGIN")
	mustExecStatement(db, "SET a bar")
	claim.Equal("bar", mustGet(db, "a"))
	mustExecStatement(db, "SET a baz")
	mustExecStatement(db, "ROLLBACK")
	claim.Equal("foo", mustGet(db, "a"))
	mustExecStatement(db, "ROLLBACK")
	claim.Equal(database.NullValue, mustGet(db, "a"))
}

func TestExampleFour(t *testing.T) {
	claim := assert.New(t)
	db := database.New()
	mustExecStatement(db, "SET a foo")
	mustExecStatement(db, "SET b baz")
	mustExecStatement(db, "BEGIN")
	claim.Equal("foo", mustGet(db, "a"))
	mustExecStatement(db, "SET a bar")
	claim.NoError(assertCount(db, "bar", 1))
	mustExecStatement(db, "BEGIN")
	claim.NoError(assertCount(db, "bar", 1))
	mustExecStatement(db, "DELETE a")
	claim.Equal(database.NullValue, mustGet(db, "a"))
	claim.NoError(assertCount(db, "bar", 0))
	mustExecStatement(db, "ROLLBACK")
	claim.Equal("bar", mustGet(db, "a"))
	claim.NoError(assertCount(db, "bar", 1))
	mustExecStatement(db, "COMMIT")
	claim.Equal("bar", mustGet(db, "a"))
	claim.Equal("baz", mustGet(db, "b"))
}

func mustGet(db *database.Database, name string) string {
	text := "GET " + name
	stmt, err := parser.Parse(text)
	if err != nil {
		panic(err)
	}
	result, err := run(db, stmt)
	if err != nil {
		panic(err)
	}
	return result
}

func assertCount(db *database.Database, value string, expected int) error {
	text := "COUNT " + value
	stmt, err := parser.Parse(text)
	if err != nil {
		return fmt.Errorf("parsing of text %q has failed: %w", text, err)
	}
	result, err := run(db, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute statement %s on database: %w", text, err)
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

func mustExecStatement(db *database.Database, input string) {
	stmt, err := parser.Parse(input)
	if err != nil {
		panic(fmt.Sprintf("failed to parse %q: %v", input, err))
	}
	if _, err := run(db, stmt); err != nil {
		panic(err)
	}
}

package parser

import (
	"fmt"
	"strings"
)

const (
	BeginOp    = "BEGIN"
	CommitOp   = "COMMIT"
	DeleteOp   = "DELETE"
	GetOp      = "GET"
	EndOp      = "END"
	RollbackOp = "ROLLBACK"
	CountOp    = "COUNT"
	SetOp      = "SET"
)

var (
	supportedOps = []string{
		BeginOp, CommitOp, DeleteOp, GetOp, EndOp, RollbackOp, SetOp, CountOp,
	}
)

type Statement struct {
	Name      string
	Value     string
	Operation string
}

func Parse(input string) (*Statement, error) {
	stmt := &Statement{}
	parts := strings.Split(input, " ")
	stmt.Operation = parts[0]
	if stmt.Operation == "" {
		return nil, fmt.Errorf("database operation is not specified: statement must start with one of the following: " +
			strings.Join(supportedOps, " "))
	}
	switch stmt.Operation {
	case DeleteOp:
		if len(parts) < 2 || parts[1] == "" {
			return nil, fmt.Errorf("failed to execute operation %s: statement %q does not have a name",
				DeleteOp, input)
		}
		stmt.Name = parts[1]
		return stmt, nil
	case GetOp:
		if len(parts) < 2 || parts[1] == "" {
			return stmt, fmt.Errorf("failed to execute operation %s: statement %q does not have a name",
				GetOp, input)
		}
		stmt.Name = parts[1]
		return stmt, nil
	case SetOp:
		if len(parts) < 3 || (parts[1] == "" || parts[2] == "") {
			return nil, fmt.Errorf("failed to execute operation %s: statement %q does not have a name or value",
				SetOp, input)
		}
		stmt.Name = parts[1]
		stmt.Value = parts[2]
	case CountOp:
		if len(parts) < 2 || parts[1] == "" {
			return nil, fmt.Errorf("failed to execute operation %s: statement %q does not have a value",
				CountOp, strings.Join(parts, " "))
		}
		stmt.Value = parts[1]
		return stmt, nil
	case BeginOp, CommitOp, EndOp, RollbackOp:
		return stmt, nil
	default:
		return nil, fmt.Errorf("database does not support operation %s: supported operations: %s",
			stmt.Operation, strings.Join(supportedOps, " "))
	}
	return stmt, nil
}

func mustParse(text string) *Statement {
	result, err := Parse(text)
	if err != nil {
		panic(err)
	}
	return result
}

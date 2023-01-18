package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/jaloren/memdb/database"
	"github.com/jaloren/memdb/parser"
	"os"
	"strconv"
	"strings"
)

func main() {
	db := database.New()
	for {
		text := prompt(">>")
		stmt, err := parser.Parse(text)
		if err != nil {
			fmt.Println(err)
			continue
		}
		result, err := run(db, stmt)
		if errors.Is(err, database.EndOpErr) {
			fmt.Println(err)
			os.Exit(0)
		} else if err != nil {
			fmt.Println(err)
			continue
		}
		if result != "" {
			fmt.Println(result)
		}
	}
}

func run(db *database.Database, stmt *parser.Statement) (string, error) {
	switch stmt.Operation {
	case parser.DeleteOp:
		db.Transaction.Delete(stmt.Name)
		return "", nil
	case parser.GetOp:
		return db.Transaction.Get(stmt.Name), nil
	case parser.SetOp:
		db.Transaction.Set(stmt.Name, stmt.Value)
		return "", nil
	case parser.CountOp:
		return strconv.Itoa(db.Transaction.Count(stmt.Value)), nil
	case parser.BeginOp:
		db.Transaction = db.Transaction.Begin()
		return "", nil
	case parser.CommitOp:
		db.Transaction = db.Transaction.Commit()
		return "", nil
	case parser.EndOp:
		return "", database.EndOpErr
	case parser.RollbackOp:
		prev, err := db.Transaction.Rollback()
		if err != nil {
			return "", err
		}
		db.Transaction = prev
		return "", nil
	}
	return "", fmt.Errorf("database does not support operation %s", stmt.Operation)
}

func prompt(label string) string {
	var s string
	r := bufio.NewReader(os.Stdin)
	for {
		fmt.Fprint(os.Stderr, label+" ")
		s, _ = r.ReadString('\n')
		if s != "" {
			break
		}
	}
	return strings.TrimSpace(s)
}

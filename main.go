package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/jaloren/memdb/database"
	"os"
	"strings"
)

func main() {
	db := database.New()
	for {
		text := prompt(">>")
		result, err := db.ExecStatement(text)
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

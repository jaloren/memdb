package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAndGet(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	val := "second"
	db.transaction.Set(key, val)
	claim.Equal(val, db.transaction.Get(key))
}

func TestUpdate(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	firstVal := "second"
	db.transaction.Set(key, firstVal)
	claim.Equal(firstVal, db.transaction.Get(key))
	secondVal := "third"
	db.transaction.Set(key, secondVal)
	claim.Equal(secondVal, db.transaction.Get(key))
}

func TestDelete(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	val := "second"
	db.transaction.Set(key, val)
	claim.Equal(val, db.transaction.Get(key))
	db.transaction.Delete(key)
	claim.Equal(nullValue, db.transaction.Get(key))
}

func TestCount(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.transaction.Set("aaa", "second")
	db.transaction.Set("aaa", "second")
	db.transaction.Set("aaa", "second")
	db.transaction.Set("aaa", "xxx")
	db.transaction.Set("bbb", "second")
	db.transaction.Set("fff", "first")
	db.transaction.Set("ggg", "third")
	db.transaction.Set("hhh", "fourth")
	db.transaction.Set("iii", "eleven")
	db.transaction.Set("jjj", "twelve")

	claim.Equal(1, db.transaction.Count("second"))
	db.transaction.Delete("bbb")
	claim.Equal(1, db.transaction.Count("twelve"))
}

func TestCountWithTransaction(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.transaction.Set("a", "foo")
	db.transaction.Begin()
	db.transaction.Delete("a")
	db.transaction.Delete("a")
	db.transaction.Set("a", "foo")
	db.transaction.Set("b", "foo")
	claim.Equal(2, db.transaction.Count("foo"))
}

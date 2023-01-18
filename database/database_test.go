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
	db.Transaction.Set(key, val)
	claim.Equal(val, db.Transaction.Get(key))
}

func TestUpdate(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	firstVal := "second"
	db.Transaction.Set(key, firstVal)
	claim.Equal(firstVal, db.Transaction.Get(key))
	secondVal := "third"
	db.Transaction.Set(key, secondVal)
	claim.Equal(secondVal, db.Transaction.Get(key))
}

func TestDelete(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	val := "second"
	db.Transaction.Set(key, val)
	claim.Equal(val, db.Transaction.Get(key))
	db.Transaction.Delete(key)
	claim.Equal(NullValue, db.Transaction.Get(key))
}

func TestCount(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.Transaction.Set("aaa", "second")
	db.Transaction.Set("aaa", "second")
	db.Transaction.Set("aaa", "second")
	db.Transaction.Set("aaa", "xxx")
	db.Transaction.Set("bbb", "second")
	db.Transaction.Set("fff", "first")
	db.Transaction.Set("ggg", "third")
	db.Transaction.Set("hhh", "fourth")
	db.Transaction.Set("iii", "eleven")
	db.Transaction.Set("jjj", "twelve")

	claim.Equal(1, db.Transaction.Count("second"))
	db.Transaction.Delete("bbb")
	claim.Equal(1, db.Transaction.Count("twelve"))
}

func TestCountWithTransaction(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.Transaction.Set("a", "foo")
	db.Transaction.Begin()
	db.Transaction.Delete("a")
	db.Transaction.Delete("a")
	db.Transaction.Set("a", "foo")
	db.Transaction.Set("b", "foo")
	claim.Equal(2, db.Transaction.Count("foo"))
}

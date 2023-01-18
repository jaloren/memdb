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
	db.Set(key, val)
	claim.Equal(val, db.Get(key))
}

func TestUpdate(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	firstVal := "second"
	db.Set(key, firstVal)
	claim.Equal(firstVal, db.Get(key))
	secondVal := "third"
	db.Set(key, secondVal)
	claim.Equal(secondVal, db.Get(key))
}

func TestDelete(t *testing.T) {
	claim := assert.New(t)
	db := New()
	key := "first"
	val := "second"
	db.Set(key, val)
	claim.Equal(val, db.Get(key))
	db.Delete(key)
	claim.Equal(nullValue, db.Get(key))
}

func TestCount(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.Set("aaa", "second")
	db.Set("aaa", "second")
	db.Set("aaa", "second")
	db.Set("aaa", "xxx")
	db.Set("bbb", "second")
	db.Set("fff", "first")
	db.Set("ggg", "third")
	db.Set("hhh", "fourth")
	db.Set("iii", "eleven")
	db.Set("jjj", "twelve")

	claim.Equal(1, db.Count("second"))
	db.Delete("bbb")
	claim.Equal(1, db.Count("twelve"))
}

func TestCountWithTransaction(t *testing.T) {
	claim := assert.New(t)
	db := New()
	db.Set("a", "foo")
	db.BeginTransaction()
	db.Delete("a")
	db.Delete("a")
	db.Set("a", "foo")
	db.Set("b", "foo")
	//runtime.Breakpoint()
	claim.Equal(2, db.Count("foo"))
}

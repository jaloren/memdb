package database

type transaction struct {
	valCnt map[string]int
	data   map[string]*string
	prev   *transaction
}

func newTransaction(prev *transaction) *transaction {
	return &transaction{
		valCnt: make(map[string]int),
		data:   make(map[string]*string),
		prev:   prev,
	}
}

func (t *transaction) get(name string) (string, bool) {
	val, ok := t.data[name]
	if !ok {
		return "", false
	}
	if val != nil {
		return deref(val), true
	}
	return nullValue, true
}

// SET a foo
// BEGIN
// SET b foo
// DELETE a
// BEGIN
// SET c foo

// BEGIN
// SET a foo

func (t *transaction) add(db *Database, name string, rawValue *string) {
	t.data[name] = rawValue
	current := t

	var deletedRawValue *string
	for {
		existingRawVal, ok := current.data[name]
		if !ok {
			current = t.prev
			if current == nil {
				break
			}
			continue
		}
		if deletedRawValue == nil && existingRawVal != nil {
			deletedRawValue = existingRawVal
		}
		currentCnt, ok := current.valCnt[deref(existingRawVal)]
		if !ok {
			current = t.prev
			if current == nil {
				break
			}
			continue
		}
		if rawValue == nil {
			if currentCnt > 0 {
				t.valCnt[deref(existingRawVal)] = currentCnt - 1
			}

			return
		} else if rawValue != nil {
			t.valCnt[deref(rawValue)] = currentCnt + 1
			return
		} else {
			current = t.prev
			if current == nil {
				break
			}
		}
	}
	if deletedRawValue == nil {
		deletedRawValue = db.data[name]
	}

	if currentCnt, ok := db.valCnt[deref(deletedRawValue)]; ok {
		if rawValue == nil {
			if currentCnt > 0 {
				t.valCnt[deref(deletedRawValue)] = currentCnt - 1
			}

			return
		} else {
			t.valCnt[deref(rawValue)] = currentCnt + 1
			return
		}
	}

	if rawValue != nil {
		t.valCnt[deref(rawValue)] = 1
	}
}

func transValuesAreEqual(first, second *string) bool {
	if first == nil && second == nil {
		return true
	}
	return deref(first) == deref(second)
}

func (t *transaction) increValCnt(rawValue *string) {
	value := deref(rawValue)
	cnt, ok := t.valCnt[value]
	if ok {
		t.valCnt[value] = cnt + 1
	} else {
		t.valCnt[value] = 1
	}
}

func (t *transaction) decreValCnt(rawVal *string) {
	val := deref(rawVal)
	cnt, ok := t.valCnt[val]
	if ok {
		t.valCnt[val] = cnt - 1
	}
}

func ptr(input string) *string {
	return &input
}
func deref(input *string) string {
	if input == nil {
		return nullValue
	}
	return *input
}

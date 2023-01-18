## How to Run the Program

The following instructions assume you know how to use a terminal.

1. if you do not already have the go toolchain, follow these [install instructions](https://go.dev/doc/install).
2. Verify the go command is on your path. If it's not, add it.
3. open a terminal on your computer. The remaining instructions will occur in this terminal.
4. git clone this repo.
5. cd into the directory that contains the root of this repo.
6. execute the command: `go run .`

## Design Notes

This is an in-memory database that is a CLI program that takes commands from stdin. The only data type supported are
string keys and values. It is not safe for concurrent access but that's fine since its in-memory that can only be modified 
via STDIN is single threaded. The database supports: getting value by get (GET), setting a value with name (SET), 
counting the number of values in the database (count), removing a value by name (DELETE), and transactions.

Some key aspects of the design are:

- I am using maps. concerning time complexity of the operations on maps. In worst case, it can be O(t) where t is opened 
  transactions but that's worse case and in the normal case it's very likely to be O(n):
  - get and count: O(t) because of open transactions but on initial transaction is O(1)
  - delete: and count O(1) because delete on maps is O(1)
    O(t) because map lookups on Go maps is O(1)
  - set: O(n) worst case in case need to resize map; O(1) when no need to resize map
  - If someone is willing to decreate performance in the normal case O(1) to O(log(n)), then i suspect 
    you could move to red-black tree. This means that normal case is slower but the worse case is much more performant.
- The database struct has two maps: name to value and value to names. In the later case, the value is a key to a set
 of names where each name points to the value. This effectively lets me implement a bidirectional map (e.g [Guava BiMap](https://guava.dev/releases/19.0/api/docs/com/google/common/collect/BiMap.html))
- The name-to-value map is what get/set/delete operations work on. The value to names map is used for counting values
- The database struct has a pointer to a transaction(TXN) struct. The TXN struct has a pointer 
  to the database and a pointer to the previous TXN. In the txn struct, the db pointer can be nil, the prev txn pointer can be nil
  but never both. These semantics give me a linked list that allows me to go from the latest tnx to the root txn
  recursively. This makes it trivial to do rollbacks: just return the previous txn or itself.
- There's always at least one txn which is the implicit one that is executed when a BEGIN command has not yet been executed.
  If operations are executed on the root txn, then that tnx mutates the maps on the database struct. If one or more BEGIN commands 
   has been executed, then operations on the current transaction will update it's version of the bidirectional mappings for names
 and values and then where appropriate recursively call the same operations on all previous tnxs.

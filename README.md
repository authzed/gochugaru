<img align="right" width="100" height="100" alt="DALL·E 2024-01-10 01 10 19 - the go gopher holding a jar of gochugaru" src="https://github.com/authzed/gochugaru/assets/343539/67bb8a28-d425-472f-96ec-2abbe2982ed2"/>

# gochugaru

[![GoDoc](https://godoc.org/github.com/authzed/gochugaru?status.svg)](https://godoc.org/github.com/authzed/gochugaru)
[![Docs](https://img.shields.io/badge/docs-authzed.com-%234B4B6C "Authzed Documentation")](https://authzed.com/docs)
[![YouTube](https://img.shields.io/youtube/channel/views/UCFeSgZf0rPqQteiTQNGgTPg?color=%23F40203&logo=youtube&style=flat-square&label=YouTube "Authzed YouTube Channel")](https://www.youtube.com/channel/UCFeSgZf0rPqQteiTQNGgTPg)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://authzed.com/discord)
[![Twitter](https://img.shields.io/badge/twitter-%40authzed-1D8EEE?logo=twitter "@authzed on Twitter")](https://twitter.com/authzed)

A SpiceDB client library striving to be as ergonomic as possible.

This library builds upon the official [authzed-go library], but tries to expose an interface that guides folks towards optimal performance and correctness.

[authzed-go library]: https://github.com/authzed/authzed-go

## Roadmap

### UX

- ✅ Security-obvious client constructors
- ✅ Defaults to SpiceDB's best compression method
- ✅ Automatic back-off & retry logic
- ✅ Check One/Many/Any/All methods
- ✅ Checks use BulkChecks under the hood
- ✅ Interfaces for Relationships, Objects
- ✅ Flattened Relationship-type with Caveats
- ✅ Transaction-style API for Write
- ✅ Constructors for consistency arguments
- ✅ Callback-style API for Watch and ReadRelationships
- ✅ Atomic and non-atomic Relationship deletion
- 🔜 Keepalives for watch (if necessary)

### APIs

- ✅ Checks
- ✅ Schema Read/Write
- ✅ Relationship Read/Write/Delete
- 🚧 Import/Export Relationships
- ✅ Watch
- 🔜 Request Debugging
- 🔜 Lookup Resources/Subjects
- 🔜 Reflection APIs

## Examples

### Clients

```go
import "github.com/authzed/gochugaru/client"

...

// Various constructors to allocate clients for dev and production environments
// using the best practices.
authz, err := client.NewSystemTLS("spicedb.mycluster.local", presharedKey)
if err != nil {
  ...
}
```

### Checks

```go
import "github.com/authzed/gochugaru/client"
import "github.com/authzed/gochugaru/rel"

...

// Build up a set of relationships to be checked like any other slice.
var founders []Relationship
for _, founder := range []string{"jake", "joey", "jimmy"} {
  // There are various constructors for the Relationship type that can
  // trade-off allocations for legibility and understandability.
  rel, err := rel.FromTriple("company:authzed", "founder", "user:"+founder)
  if err != nil {
    ...
  }
  founders = append(founders, rel)
}

// Various Check methods can be used to simplify common assertions.
allAreFounders, err := authz.CheckAll(ctx, consistency.MinLatency(), founders...)
if err != nil {
  ...
} else if !allAreFounders {
  ...
}
```

### Writes

```go
import "github.com/authzed/gochugaru/client"
import "github.com/authzed/gochugaru/rel"

...

// Transactions are built up of preconditions that must or must not exist and
// the set of updates (creates, touches, or deletes) to be applied.
var txn rel.Txn

// The preconditions:
for _, rival := range []string{"joey", "jake"} {
  txn.MustNotMatch(rel.MustFromTriple("module:gochugaru", "creator", "user:"+rival).Filter())
}

// The updates:
txn.Touch(rel.MustFromTriple("module:gochugaru", "creator", "user:jimmy"))
txn.Touch(rel.MustFromTriple("module:gochugaru", "maintainer", "sam").
	WithCaveat("on_tuesday", map[string]any{"day": "wednesday"}))

writtenAt, err := authz.Write(ctx, txn)
...
```

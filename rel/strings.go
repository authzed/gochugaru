package rel

import (
	"errors"
	"strings"
)

var (
	ErrInvalidObjectString        = errors.New("invalid object string: must be in form `objectType:objectID#optionalRelation`")
	ErrInvalidTypedRelationString = errors.New("invalid typed permission string: must be in form `objectType#relation`")
)

// ParseObjectSet returns the composite parts of an object string including an
// optional relation for sets.
//
// Examples:
// `document:README` => ("document", "README", "", nil)
// `document:README#reader` => ("document", "README", "reader", nil)
func ParseObjectSet(obj string) (objectType, objectID, relation string, err error) {
	var found bool
	objectType, objectID, found = strings.Cut(obj, ":")
	if !found {
		err = ErrInvalidObjectString
		return
	}
	objectID, relation, _ = strings.Cut(objectID, "#")
	return
}

// ParseTypedRelation
func ParseTypedRelation(perm string) (objectType, relation string, err error) {
	var found bool
	objectType, relation, found = strings.Cut(perm, "#")
	if !found {
		err = ErrInvalidTypedRelationString
	}
	return
}

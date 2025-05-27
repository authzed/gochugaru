package rel

import (
	"errors"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	// ErrInvalidResource is a catch-all error when a resource is invalid.
	ErrInvalidResource = errors.New("invalid resource")

	// ErrInvalidRelation is a catch-all error when a relation is invalid.
	ErrInvalidRelation = errors.New("invalid relation")

	// ErrInvalidSubject is a catch-all error when a subject is invalid.
	ErrInvalidSubject = errors.New("invalid subject")
)

type Interface interface{ Relationship() Relationship }

type Relationship struct {
	ResourceType     string
	ResourceID       string
	ResourceRelation string
	SubjectType      string
	SubjectID        string
	SubjectRelation  string
	CaveatName       string
	CaveatContext    map[string]any
}

func (r Relationship) Relationship() Relationship { return r }
func (r Relationship) Permission() string         { return r.ResourceRelation }
func (r Relationship) HasCaveat() bool            { return r.CaveatName != "" }

func (r Relationship) Caveat() (name string, context map[string]any, exists bool) {
	return r.CaveatName, r.CaveatContext, r.HasCaveat()
}

func (r Relationship) String() string {
	var b strings.Builder
	b.WriteString(r.ResourceType)
	b.WriteString(":")
	b.WriteString(r.ResourceID)
	b.WriteString("#")
	b.WriteString(r.ResourceRelation)
	b.WriteString("@")
	b.WriteString(r.SubjectType)
	b.WriteString(":")
	b.WriteString(r.SubjectID)
	if r.SubjectRelation != "" {
		b.WriteString("#")
		b.WriteString(r.SubjectRelation)
	}
	if r.HasCaveat() {
		b.WriteString("[")
		b.WriteString(r.CaveatName)
		if len(r.CaveatContext) > 0 {
			cc, err := structpb.NewStruct(r.CaveatContext)
			if err != nil {
				panic("caveat created with non-utf8 context key")
			}

			b.WriteString(":")
			ccBytes, err := cc.MarshalJSON()
			if err != nil {
				panic(err)
			}
			b.Write(ccBytes)
		}
		b.WriteString("]")
	}
	return b.String()
}

func (r Relationship) WithCaveat(name string, context map[string]any) Relationship {
	return Relationship{
		ResourceType:     r.ResourceType,
		ResourceID:       r.ResourceID,
		ResourceRelation: r.ResourceRelation,
		SubjectType:      r.SubjectType,
		SubjectID:        r.SubjectID,
		SubjectRelation:  r.SubjectRelation,
		CaveatName:       name,
		CaveatContext:    context,
	}
}

func (r Relationship) Filter() *Filter {
	f := NewFilter(r.ResourceType, r.ResourceID, r.ResourceRelation)
	f.WithSubjectFilter(r.SubjectType, r.SubjectID, r.SubjectRelation)
	return f
}

func (r Relationship) v1() *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: r.ResourceType,
			ObjectId:   r.ResourceID,
		},
		Relation: r.ResourceRelation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: r.SubjectType,
				ObjectId:   r.SubjectID,
			},
			OptionalRelation: r.SubjectRelation,
		},
		OptionalCaveat: r.MustV1ProtoCaveat(),
	}
}

func FromV1Proto(r *v1.Relationship) *Relationship {
	var caveatName string
	var caveatContext map[string]any
	if r.OptionalCaveat != nil {
		caveatName = r.OptionalCaveat.CaveatName
		caveatContext = r.OptionalCaveat.Context.AsMap()
	}

	return &Relationship{
		ResourceType:     r.Resource.ObjectType,
		ResourceID:       r.Resource.ObjectId,
		ResourceRelation: r.Relation,
		SubjectType:      r.Subject.Object.ObjectType,
		SubjectID:        r.Subject.Object.ObjectId,
		SubjectRelation:  r.Subject.OptionalRelation,
		CaveatName:       caveatName,
		CaveatContext:    caveatContext,
	}
}

func (r Relationship) MustV1ProtoCaveat() *v1.ContextualizedCaveat {
	if name, context, ok := r.Caveat(); ok {
		cc, err := structpb.NewStruct(context)
		if err != nil {
			panic("caveat created with non-utf8 context key")
		}

		return &v1.ContextualizedCaveat{
			CaveatName: name,
			Context:    cc,
		}
	}

	return nil
}

type Object struct {
	Type     string
	ID       string
	Relation string
}

type Subject Object

func (o Object) Object() Object { return o }

type Objecter interface{ Object() Object }

func FromObjects(resource, subject Objecter) Relationship {
	r, s := resource.Object(), subject.Object()
	return Relationship{
		ResourceType:     r.Type,
		ResourceID:       r.ID,
		ResourceRelation: r.Relation,
		SubjectType:      s.Type,
		SubjectID:        s.ID,
		SubjectRelation:  s.Relation,
	}
}

func MustFromTriple(resource, relation, subject string) Relationship {
	r, err := FromTriple(resource, relation, subject)
	if err != nil {
		panic(err)
	}
	return r
}

func FromTriple(resource, relation, subject string) (Relationship, error) {
	return FromTuple(resource+"#"+relation, subject)
}

func MustFromTuple(resource, subject string) Relationship {
	r, err := FromTuple(resource, subject)
	if err != nil {
		panic(err)
	}
	return r
}

func FromTuple(resource, subject string) (Relationship, error) {
	var (
		r     Relationship
		found bool
	)

	resource, r.ResourceRelation, found = strings.Cut(resource, "#")
	if !found || r.ResourceRelation == "" {
		return r, ErrInvalidRelation
	}

	r.ResourceType, r.ResourceID, found = strings.Cut(resource, ":")
	if !found {
		return r, ErrInvalidResource
	}

	// Optional
	subject, r.SubjectRelation, _ = strings.Cut(subject, "#")

	r.SubjectType, r.SubjectID, found = strings.Cut(subject, ":")
	if !found {
		return r, ErrInvalidSubject
	}

	return r, nil
}

type Func func(r *Relationship) error

type UpdateType int

const (
	UpdateUnknown UpdateType = iota
	UpdateCreate
	UpdateDelete
	UpdateTouch
)

type UpdateFunc func(typ UpdateType, r *Relationship) error

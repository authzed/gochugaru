// Package client implements an ergonomic SpiceDB client that wraps the
// official AuthZed gRPC client.
package client

import (
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/cenkalti/backoff/v5"
	"github.com/jzelinskie/itz"
	_ "github.com/mostynb/go-grpc-compression/experimental/s2" // Register Snappy S2 compression
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/gochugaru/consistency"
	"github.com/authzed/gochugaru/rel"
)

var defaultClientOpts = []grpc.DialOption{
	grpc.WithDefaultCallOptions(grpc.UseCompressor("s2")),
}

// NewPlaintext creates a client that does not enforce TLS.
//
// This should be used only for testing (usually against localhost).
func NewPlaintext(endpoint, presharedKey string) (*Client, error) {
	return NewWithOpts(endpoint, WithDialOpts(append(
		defaultClientOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(presharedKey),
	)...))
}

// NewSystemTLS creates a client using TLS verified by the operating
// system's certificate chain.
//
// This should be sufficient for production usage in the typical environments.
func NewSystemTLS(endpoint, presharedKey string) (*Client, error) {
	withSystemCerts, err := grpcutil.WithSystemCerts(grpcutil.VerifyCA)
	if err != nil {
		return nil, err
	}

	return NewWithOpts(endpoint, WithDialOpts(append(
		defaultClientOpts,
		withSystemCerts,
		grpcutil.WithBearerToken(presharedKey),
	)...))
}

// NewWithOpts creates a new client with the defaults overwritten with any
// provided options.
func NewWithOpts(endpoint string, opts ...Option) (*Client, error) {
	c := newWithDefaults()
	for _, opt := range opts {
		opt(c)
	}

	if err := c.connect(endpoint); err != nil {
		return nil, err
	}
	return c, nil
}

type Option func(client *Client)

// WithOverlapRequired will cause a panic if a request is made that does not
// provide an overlap key.
//
// This should be used to prevent any issues when SpiceDB is configured for
// overlap keys provided from requests.
func WithOverlapRequired() Option {
	return func(client *Client) { client.overlapRequired = true }
}

// WithDialOpts allows for configuring the underlying gRPC connection.
//
// This should only be used if the other methods don't suffice.
//
// We'd also love to hear about which DialOptions you're using in the
// SpiceDB Discord (https://discord.gg/spicedb) or the issue tracker for this
// library.
func WithDialOpts(opts ...grpc.DialOption) Option {
	return func(client *Client) { client.dialOpts = opts }
}

type RequestOption func(context.Context)

type Client struct {
	client          *authzed.ClientWithExperimental
	dialOpts        []grpc.DialOption
	overlapRequired bool
}

func newWithDefaults() *Client {
	return &Client{dialOpts: defaultClientOpts}
}

func (c *Client) connect(endpoint string) (err error) {
	c.client, err = authzed.NewClientWithExperimentalAPIs(endpoint, c.dialOpts...)
	return err
}

// Write atomically performs a transaction on relationships.
func (c *Client) Write(ctx context.Context, txn rel.Txn) (writtenAtRevision string, err error) {
	resp, err := c.client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates:               txn.V1Updates,
		OptionalPreconditions: txn.V1Preconds,
	})
	if err != nil {
		return "", err
	}
	return resp.WrittenAt.Token, nil
}

// CheckOne performs a permissions check for a single relationship.
func (c *Client) CheckOne(ctx context.Context, cs *consistency.Strategy, r rel.Interface) (bool, error) {
	results, err := c.Check(ctx, cs, r)
	if err != nil {
		return false, err
	}
	return results[0], nil
}

// CheckAny returns true if any of the provided relationships have access.
func (c *Client) CheckAny(ctx context.Context, cs *consistency.Strategy, rs ...rel.Interface) (bool, error) {
	results, err := c.Check(ctx, cs, rs...)
	if err != nil {
		return false, err
	}

	return slices.Contains(results, true), nil
}

// CheckAll returns true if all of the provided relationships have access.
func (c *Client) CheckAll(ctx context.Context, cs *consistency.Strategy, rs ...rel.Interface) (bool, error) {
	results, err := c.Check(ctx, cs, rs...)
	if err != nil {
		return false, err
	}

	for _, result := range results {
		if !result {
			return false, nil
		}
	}
	return true, nil
}

// CheckIter iterates over the provided relationships, batching them into
// requests, and returns an iterator of their results.
func (c *Client) CheckIter(ctx context.Context, cs *consistency.Strategy, rs iter.Seq[rel.Interface]) iter.Seq2[bool, error] {
	return func(yield func(bool, error) bool) {
		for items := range itz.Chunk(rs, 1000) {
			checks, err := c.Check(ctx, cs, items...)
			if err != nil {
				yield(false, err)
				return
			}

			for _, check := range checks {
				if !yield(check, nil) {
					return
				}
			}
		}
	}
}

func (c *Client) checkOverlap(ctx context.Context) {
	if c.overlapRequired {
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			if len(md.Get(string(requestmeta.RequestOverlapKey))) > 0 {
				return
			}
		}
		panic("failed to configure required overlap key for request")
	}
}

func retryRetriableErrors[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	retriableFunc := func() (T, error) {
		result, err := fn()
		if isGrpcCode(err, codes.Unavailable, codes.DeadlineExceeded) ||
			errContainsAny(err, "retryable error", "try restarting transaction") || // SpiceDB < v1.30 need this to properly retry.
			errors.Is(err, context.DeadlineExceeded) {
			return result, err
		}

		return result, backoff.Permanent(err)
	}

	return backoff.Retry(ctx, retriableFunc, backoff.WithBackOff(&backoff.ExponentialBackOff{
		InitialInterval:     50 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         2 * time.Second,
	}))
}

func isGrpcCode(err error, codes ...codes.Code) bool {
	if err == nil {
		return false
	}

	if s, ok := status.FromError(err); ok {
		return slices.Contains(codes, s.Code())
	}
	return false
}

func errContainsAny(err error, errStrs ...string) bool {
	if err == nil {
		return false
	}

	for _, errStr := range errStrs {
		if strings.Contains(err.Error(), errStr) {
			return true
		}
	}
	return false
}

// Check performs a batched permissions check for the provided relationships.
func (c *Client) Check(ctx context.Context, cs *consistency.Strategy, rs ...rel.Interface) ([]bool, error) {
	c.checkOverlap(ctx)

	items := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(rs))
	for _, ir := range rs {
		r := ir.Relationship()
		items = append(items, &v1.CheckBulkPermissionsRequestItem{
			Resource: &v1.ObjectReference{
				ObjectType: r.ResourceType,
				ObjectId:   r.ResourceID,
			},
			Permission: r.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: r.SubjectType,
					ObjectId:   r.SubjectID,
				},
				OptionalRelation: r.SubjectRelation,
			},
			Context: r.MustV1ProtoCaveat().GetContext(),
		})
	}

	resp, err := retryRetriableErrors(ctx, func() (*v1.CheckBulkPermissionsResponse, error) {
		return c.client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
			Consistency: cs.V1Consistency,
			Items:       items,
		})
	})
	if err != nil {
		return nil, err
	}

	var results []bool
	for _, pair := range resp.Pairs {
		switch resp := pair.Response.(type) {
		case *v1.CheckBulkPermissionsPair_Item:
			results = append(
				results,
				resp.Item.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			)
		case *v1.CheckBulkPermissionsPair_Error:
			return results, errors.New(resp.Error.Message)
		}
	}
	return results, nil
}

// ReadRelationships returns an iterator for all of the relationships matching
// the provided filter.
func (c *Client) ReadRelationships(ctx context.Context, cs *consistency.Strategy, f *rel.Filter) iter.Seq2[*rel.Relationship, error] {
	c.checkOverlap(ctx)

	return func(yield func(*rel.Relationship, error) bool) {
		stream, err := c.client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
			Consistency:        cs.V1Consistency,
			RelationshipFilter: f.V1Filter,
			OptionalLimit:      512,
		})
		if err != nil {
			yield(nil, err)
			return
		}

		for msg, err := stream.Recv(); !errors.Is(err, io.EOF); msg, err = stream.Recv() {
			if err != nil {
				yield(nil, err)
				return
			} else if err := stream.Context().Err(); err != nil {
				yield(nil, err)
				return
			}
			if !yield(rel.FromV1Proto(msg.Relationship), nil) {
				return
			}
		}
	}
}

// DeleteAtomic removes all of the relationships matching the provided filter
// in a single transaction.
func (c *Client) DeleteAtomic(ctx context.Context, f *rel.PreconditionedFilter) (deletedAtRevision string, err error) {
	c.checkOverlap(ctx)

	// Explicitly given no back-off or retry logic.
	resp, err := c.client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
		RelationshipFilter:            f.V1Filter,
		OptionalPreconditions:         f.V1Preconds,
		OptionalLimit:                 0,
		OptionalAllowPartialDeletions: false,
	})
	if err != nil {
		return "", err
	} else if resp.DeletionProgress != v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE {
		return "", errors.New("delete disallowing partial deletion did not complete")
	}

	return resp.DeletedAt.Token, nil
}

// Delete removes all of the relationships matching the provided filter in
// batches.
func (c *Client) Delete(ctx context.Context, f *rel.PreconditionedFilter) error {
	c.checkOverlap(ctx)

	for {
		resp, err := retryRetriableErrors(ctx, func() (*v1.DeleteRelationshipsResponse, error) {
			return c.client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
				RelationshipFilter:            f.V1Filter,
				OptionalPreconditions:         f.V1Preconds,
				OptionalLimit:                 10_000,
				OptionalAllowPartialDeletions: false,
			})
		})
		if err != nil {
			return err
		} else if resp.DeletionProgress == v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE {
			return nil
		}
	}
}

// Updates returns an iterator that's subscribed to optionally-filtered updates
// from the SpiceDB Watch API.
//
// This function can and should be cancelled via context.
func (c *Client) Updates(ctx context.Context, f rel.UpdateFilter) iter.Seq2[rel.Update, error] {
	return c.UpdatesSinceRevision(ctx, f, "")
}

// UpdatesSinceRevision is a variation of the Updates method that supports
// starting the iterator at a specific revision.
func (c *Client) UpdatesSinceRevision(ctx context.Context, f rel.UpdateFilter, revision string) iter.Seq2[rel.Update, error] {
	c.checkOverlap(ctx)

	return func(yield func(rel.Update, error) bool) {
		v1filters := make([]*v1.RelationshipFilter, 0, len(f.RelationshipFilters))
		for _, f := range f.RelationshipFilters {
			v1filters = append(v1filters, f.V1Filter)
		}

		var zedtoken *v1.ZedToken
		if revision != "" {
			zedtoken = &v1.ZedToken{Token: revision}
		}

		watchStream, err := c.client.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes:         f.ObjectTypes,
			OptionalRelationshipFilters: v1filters,
			OptionalStartCursor:         zedtoken,
		})
		if err != nil {
			yield(rel.Update{}, err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := watchStream.Recv()
				if err != nil {
					yield(rel.Update{}, err)
					return
				}

				for _, update := range resp.Updates {
					if !yield(rel.UpdateFromV1Proto(update), nil) {
						return
					}
				}
			}
		}
	}
}

// ReadSchema reads the current schema with full consistency.
func (c *Client) ReadSchema(ctx context.Context) (schema, revision string, err error) {
	resp, err := c.client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		return schema, revision, err
	}
	return resp.SchemaText, resp.ReadAt.Token, nil
}

// WriteSchema applies the provided schema to SpiceDB.
//
// Any schema causing relationships to be unreferenced will throw an error.
// These relationships must be deleted before the schema can be valid.
func (c *Client) WriteSchema(ctx context.Context, schema string) (revision string, err error) {
	resp, err := c.client.WriteSchema(ctx, &v1.WriteSchemaRequest{Schema: schema})
	if err != nil {
		return revision, err
	}
	return resp.WrittenAt.Token, nil
}

// ImportRelationships is similar to Write, but is optimized for performing
// full restorations of SpiceDB.
func (c *Client) ImportRelationships(ctx context.Context, rs iter.Seq[rel.Interface]) error {
	stream, err := c.client.BulkImportRelationships(ctx)
	if err != nil {
		return err
	}

	v1rs := itz.Map(rs, func(ir rel.Interface) *v1.Relationship {
		return ir.Relationship().V1()
	})

	for rs := range itz.Chunk(v1rs, 1000) {
		err := stream.Send(&v1.BulkImportRelationshipsRequest{Relationships: rs})
		if isGrpcCode(err, codes.AlreadyExists) {
			if _, err := retryRetriableErrors(ctx, func() (string, error) {
				var txn rel.Txn
				for _, r := range rs {
					txn.Touch(*rel.FromV1Proto(r))
				}
				return c.Write(ctx, txn)
			}); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

// ExportRelationships is similar to ReadRelationships, but cannot be filtered
// and is optimized for performing full backups of SpiceDB.
//
// A proper backup should include relationships and schema, so this function
// should be called with the same revision as said schema.
func (c *Client) ExportRelationships(ctx context.Context, revision string) iter.Seq2[*rel.Relationship, error] {
	c.checkOverlap(ctx)

	return func(yield func(*rel.Relationship, error) bool) {
		stream, err := c.client.BulkExportRelationships(ctx, &v1.BulkExportRelationshipsRequest{
			Consistency: &v1.Consistency{Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &v1.ZedToken{Token: revision},
			}},
		})
		if err != nil {
			yield(nil, err)
			return
		}

		for msg, err := stream.Recv(); !errors.Is(err, io.EOF); msg, err = stream.Recv() {
			if err != nil {
				yield(nil, err)
				return
			}

			for _, r := range msg.Relationships {
				if !yield(rel.FromV1Proto(r), nil) {
					return
				}
			}
		}
	}
}

// LookupResources streams a sequence of resource IDs for which the provided
// subject has permission.
//
// The permission and subject are provided as strings in the following format:
// `LookupResources(ctx, consistency.MinLatency(), "document#reader" "user:jimmy")`
// or
// `LookupResources(ctx, consistency.MinLatency(), "document#reader" "team:admin#member")`
func (c *Client) LookupResources(ctx context.Context, cs *consistency.Strategy, permission, subject string) iter.Seq2[string, error] {
	c.checkOverlap(ctx)

	return func(yield func(string, error) bool) {
		subjType, subjID, subjRel, err := rel.ParseObjectSet(subject)
		if err != nil {
			yield("", err)
			return
		}

		objType, objRel, err := rel.ParseTypedRelation(permission)
		if err != nil {
			yield("", err)
			return
		}

		stream, err := c.client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: objType,
			Permission:         objRel,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: subjType,
					ObjectId:   subjID,
				},
				OptionalRelation: subjRel,
			},
			Context:     nil, // TODO(jzelinskie): maybe caveats get injected into ctx?
			Consistency: cs.V1Consistency,
		})
		if err != nil {
			yield("", err)
			return
		}

		for msg, err := stream.Recv(); !errors.Is(err, io.EOF); msg, err = stream.Recv() {
			if err != nil {
				yield("", err)
				return
			}
			if !yield(msg.ResourceObjectId, nil) {
				return
			}
		}
	}
}

// LookupSubjects streams a sequence of subject IDs of the subjects that have
// permissionship with the provided resource and permission.
//
// The string arguments are provided in the following format:
// `LookupSubjects(ctx, consistency.MinLatency(), "document:README", "editor", "user")`
// or
// `LookupSubjects(ctx, consistency.MinLatency(), "document:README", "editor", "team#member"
func (c *Client) LookupSubjects(ctx context.Context, cs *consistency.Strategy, resource, permission, subject string) iter.Seq2[string, error] {
	c.checkOverlap(ctx)

	return func(yield func(string, error) bool) {
		resType, resID, _, err := rel.ParseObjectSet(resource)
		if err != nil {
			yield("", err)
			return
		}

		subjType, subjRel, _ := strings.Cut(subject, "#")

		stream, err := c.client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
			Resource: &v1.ObjectReference{
				ObjectType: resType,
				ObjectId:   resID,
			},
			Permission:              permission,
			SubjectObjectType:       subjType,
			OptionalSubjectRelation: subjRel,
			Context:                 nil, // TODO(jzelinskie): maybe caveats get injected into ctx?
			Consistency:             cs.V1Consistency,
		})
		if err != nil {
			yield("", err)
			return
		}

		for msg, err := stream.Recv(); !errors.Is(err, io.EOF); msg, err = stream.Recv() {
			if err != nil {
				yield("", err)
				return
			}
			if !yield(msg.GetSubject().SubjectObjectId, nil) {
				return
			}
		}
	}
}

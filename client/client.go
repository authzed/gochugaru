// Package client implements an ergonomic SpiceDB client that wraps the
// official AuthZed gRPC client.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/cenkalti/backoff/v4"
	"github.com/jzelinskie/stringz"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	_ "github.com/mostynb/go-grpc-compression/experimental/s2" // Register Snappy S2 compression

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
func (c *Client) Write(ctx context.Context, txn *rel.Txn) (writtenAtRevision string, err error) {
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
func (c *Client) CheckAny(ctx context.Context, cs *consistency.Strategy, rs []rel.Interface) (bool, error) {
	results, err := c.Check(ctx, cs, rs...)
	if err != nil {
		return false, err
	}

	return slices.Contains(results, true), nil
}

// CheckAll returns true if all of the provided relationships have access.
func (c *Client) CheckAll(ctx context.Context, cs *consistency.Strategy, rs []rel.Interface) (bool, error) {
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

func (c *Client) checkOverlap(ctx context.Context) {
	if c.overlapRequired {
		if val := ctx.Value(requestmeta.RequestOverlapKey); val.(string) != "" {
			panic("failed to configure required overlap key for request")
		}
	}
}

// withBackoffRetriesAndTimeout is a utility to wrap an API call with retry
// and backoff logic based on the error or gRPC status code.
func withBackoffRetriesAndTimeout(ctx context.Context, fn func(context.Context) error) error {
	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = 50 * time.Millisecond
	backoffInterval.MaxInterval = 2 * time.Second
	backoffInterval.MaxElapsedTime = 0
	backoffInterval.Reset()

	maxRetries := 10
	defaultTimeout := 30 * time.Second

	for retryCount := 0; ; retryCount++ {
		cancelCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		err := fn(cancelCtx)
		if err == nil {
			cancel()
			return nil
		}

		cancel()

		if isRetriable(err) && retryCount < maxRetries {
			time.Sleep(backoffInterval.NextBackOff())
			retryCount++
			continue
		}
		break
	}
	return errors.New("max retries exceeded")
}

// isRetriable determines whether or not an error returned by the gRPC client
// can be retried.
func isRetriable(err error) bool {
	switch {
	case err == nil:
		return false
	case isGrpcCode(err, codes.Unavailable, codes.DeadlineExceeded):
		return true
	case errContains(err, "retryable error", "try restarting transaction"):
		return true // SpiceDB < v1.30 need this to properly retry.
	}
	return errors.Is(err, context.DeadlineExceeded)
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

func errContains(err error, errStrs ...string) bool {
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

	var foundResp *v1.CheckBulkPermissionsResponse
	if err := withBackoffRetriesAndTimeout(ctx, func(ctx context.Context) error {
		resp, err := c.client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
			Consistency: cs.V1Consistency,
			Items:       items,
		})
		if err != nil {
			return err
		}

		foundResp = resp
		return nil
	}); err != nil {
		return nil, err
	}

	var results []bool
	for _, pair := range foundResp.Pairs {
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

// FilterRelationships returns an iterator for all of the relationships
// matching the provided filter.
func (c *Client) FilterRelationships(ctx context.Context, cs *consistency.Strategy, f *rel.Filter) iter.Seq2[*rel.Relationship, error] {
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

		for resp, err := stream.Recv(); err != io.EOF; resp, err = stream.Recv() {
			if err != nil {
				yield(nil, err)
				return
			} else if err := stream.Context().Err(); err != nil {
				yield(nil, err)
				return
			}
			if !yield(rel.FromV1Proto(resp.Relationship), nil) {
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
		var resp *v1.DeleteRelationshipsResponse
		if err := withBackoffRetriesAndTimeout(ctx, func(cCtx context.Context) (cErr error) {
			resp, cErr = c.client.DeleteRelationships(cCtx, &v1.DeleteRelationshipsRequest{
				RelationshipFilter:            f.V1Filter,
				OptionalPreconditions:         f.V1Preconds,
				OptionalLimit:                 10_000,
				OptionalAllowPartialDeletions: false,
			})
			return cErr
		}); err != nil {
			return err
		} else if resp.DeletionProgress == v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE {
			break
		}
	}
	return nil
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

		req := &v1.WatchRequest{
			OptionalObjectTypes:         f.ObjectTypes,
			OptionalRelationshipFilters: v1filters,
		}
		if revision != "" {
			req.OptionalStartCursor = &v1.ZedToken{Token: revision}
		}

		watchStream, err := c.client.Watch(ctx, req)
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

// ExportRelationships is similar to ReadRelationships, but cannot be filtered
// and is optimized for performing full backups of SpiceDB.
//
// A proper backup should include relationships and schema, so this function
// should be called with the same revision as said schema.
func (c *Client) ExportRelationships(ctx context.Context, fn rel.Func, revision string) error {
	c.checkOverlap(ctx)

	relationshipStream, err := c.client.BulkExportRelationships(ctx, &v1.BulkExportRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &v1.ZedToken{Token: revision},
			},
		},
	})
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("aborted backup: %w", err)
			}

			relsResp, err := relationshipStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("error receiving relationships: %w", err)
			}

			for _, r := range relsResp.Relationships {
				if err := fn(rel.FromV1Proto(r)); err != nil {
					return err
				}
			}
		}
	}
}

// parseSubject parses the given subject string into its namespace, object ID
// and relation, if valid.
func parseSubject(s string) (subjType, id, relation string, err error) {
	err = stringz.SplitExact(s, ":", &subjType, &id)
	if err != nil {
		return
	}
	err = stringz.SplitExact(id, "#", &id, &relation)
	if err != nil {
		relation = ""
		err = nil
	}
	return
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
		subjType, subjID, subjRel, err := parseSubject(subject)
		if err != nil {
			yield("", err)
			return
		}

		objType, objRel, found := strings.Cut(permission, "#")
		if !found {
			yield("", errors.New("invalid permission arg; must be in form `objectType#relation` (e.g. document#read)"))
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
			Context:       nil, // TODO(jzelinskie): maybe caveats get injected into ctx?
			Consistency:   cs.V1Consistency,
			OptionalLimit: 512,
		})
		if err != nil {
			yield("", err)
			return
		}

		for {
			resp, err := stream.Recv()
			switch {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				yield("", err)
				return
			default:
				if !yield(resp.ResourceObjectId, nil) {
					return
				}
			}
		}
	}
}

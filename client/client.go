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

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/cenkalti/backoff/v4"
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
	return NewWithOpts(endpoint, append(
		defaultClientOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(presharedKey),
	)...)
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

	return NewWithOpts(endpoint, append(
		defaultClientOpts,
		withSystemCerts,
		grpcutil.WithBearerToken(presharedKey),
	)...)
}

// NewWithOpts creates a client that allows for configuring gRPC options.
//
// This should only be used if the other methods don't suffice.
//
// I'd love to hear about what DialOptions you're using in the SpiceDB Discord
// (https://discord.gg/spicedb) or the issue tracker for this library.
func NewWithOpts(endpoint string, opts ...grpc.DialOption) (*Client, error) {
	client, err := authzed.NewClientWithExperimentalAPIs(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

type Client struct {
	client *authzed.ClientWithExperimental
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
	items := make([]*v1.BulkCheckPermissionRequestItem, 0, len(rs))
	for _, ir := range rs {
		r := ir.Relationship()
		items = append(items, &v1.BulkCheckPermissionRequestItem{
			Resource: &v1.ObjectReference{
				ObjectType: r.ResourceType,
				ObjectId:   r.ResourceID,
			},
			Permission: r.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: r.SubjectType,
					ObjectId:   r.SubjectRelation,
				},
				OptionalRelation: r.SubjectRelation,
			},
			Context: r.MustV1ProtoCaveat().GetContext(),
		})
	}

	var resp *v1.BulkCheckPermissionResponse
	if err := withBackoffRetriesAndTimeout(ctx, func(cCtx context.Context) (cErr error) {
		resp, cErr = c.client.BulkCheckPermission(cCtx, &v1.BulkCheckPermissionRequest{
			Consistency: cs.V1Consistency,
			Items:       items,
		})
		return cErr
	}); err != nil {
		return nil, err
	}

	var results []bool
	for _, pair := range resp.Pairs {
		switch resp := pair.Response.(type) {
		case *v1.BulkCheckPermissionPair_Item:
			results = append(
				results,
				resp.Item.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			)
		case *v1.BulkCheckPermissionPair_Error:
			return results, errors.New(resp.Error.Message)
		}
	}
	return results, nil
}

// FilterRelationships returns an iterator for all of the relationships
// matching the provided filter.
func (c *Client) FilterRelationships(ctx context.Context, cs *consistency.Strategy, f *rel.Filter) (iter.Seq2[*rel.Relationship, error], error) {
	stream, err := c.client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		Consistency:        cs.V1Consistency,
		RelationshipFilter: f.V1Filter,
		// TODO(jzelinskie): handle pagination for folks
	})
	if err != nil {
		return nil, err
	}

	return func(yield func(*rel.Relationship, error) bool) {
		for resp, err := stream.Recv(); err != io.EOF; resp, err = stream.Recv() {
			if err != nil {
				yield(nil, err)
				return
			} else if err := stream.Context().Err(); err != nil {
				yield(nil, err)
			}
			yield(rel.FromV1Proto(resp.Relationship), nil)
		}
	}, nil
}

// ForEachRelationship calls the provided function for each relationship
// matching the provided filter.
func (c *Client) ForEachRelationship(ctx context.Context, cs *consistency.Strategy, f *rel.Filter, fn rel.Func) error {
	stream, err := c.client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		Consistency:        cs.V1Consistency,
		RelationshipFilter: f.V1Filter,
		// TODO(jzelinskie): handle pagination for folks
	})
	if err != nil {
		return err
	}

	for resp, err := stream.Recv(); err != io.EOF; resp, err = stream.Recv() {
		if err != nil {
			return err
		}

		if err := fn(rel.FromV1Proto(resp.Relationship)); err != nil {
			return err
		}
	}

	return nil
}

// DeleteAtomic removes all of the relationships matching the provided filter
// in a single transaction.
func (c *Client) DeleteAtomic(ctx context.Context, f *rel.PreconditionedFilter) (deletedAtRevision string, err error) {
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

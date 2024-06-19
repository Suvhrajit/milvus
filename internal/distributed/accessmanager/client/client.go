package grpcaccessmanagerclient

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/dpccvspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ types.DpcCvsAccessManagerClient = (*Client)(nil)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the access manager grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[dpccvspb.DpcCvsAccessManagerClient]
	sourceID   int64
	address    string
}

// NewClient creates a new client instance
func NewClient(ctx context.Context) types.DpcCvsAccessManagerClient {
	config := &Params.AccessManagerGrpcClientCfg
	client := &Client{
		grpcClient: grpcclient.NewClientBase[dpccvspb.DpcCvsAccessManagerClient](config, "milvus.proto.data.DpcCvsAccessManager"),
		address:    config.IP + ":" + config.Port.GetValue(),
	}
	client.grpcClient.SetRole(typeutil.AccessManagerRole)
	client.grpcClient.SetGetAddrFunc(client.getAccessManagerAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) dpccvspb.DpcCvsAccessManagerClient {
	return dpccvspb.NewDpcCvsAccessManagerClient(cc)
}

func (c *Client) getAccessManagerAddr() (string, error) {
	return c.address, nil
}

func (c *Client) Close() error {
	return c.grpcClient.Close()
}

func (c *Client) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client dpccvspb.DpcCvsAccessManagerClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

func (c *Client) GetCredentials(ctx context.Context, in *dpccvspb.GetCredentialsRequest, opts ...grpc.CallOption) (*dpccvspb.GetCredentialsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client dpccvspb.DpcCvsAccessManagerClient) (*dpccvspb.GetCredentialsResponse, error) {
		return client.GetCredentials(ctx, in)
	})
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(cvsClient dpccvspb.DpcCvsAccessManagerClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client dpccvspb.DpcCvsAccessManagerClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return call(client)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*T), err
}

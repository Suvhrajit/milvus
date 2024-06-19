package accessmanager

import (
	"context"
	grpcaccessmanagerclient "github.com/milvus-io/milvus/internal/distributed/accessmanager/client"
	dpccvdpb "github.com/milvus-io/milvus/internal/proto/dpccvspb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

type Client struct {
	Client dpccvdpb.DpcCvsAccessManagerClient
}

var (
	instance *Client
	once     sync.Once
)

func GetInstance(ctx context.Context) *Client {
	once.Do(func() {
		log.Debug("Access Manager Client initialized")
		accessManagerClient := grpcaccessmanagerclient.NewClient(ctx)
		instance = &Client{
			accessManagerClient,
		}
	})
	return instance
}

func GetCredentialsForCollection(ctx context.Context, collectionId string, bucketName string) (string, string, string, string, string, error) {
	instanceName := os.Getenv("MILVUS_INSTANCE_NAME")

	log.Debug("Milvus Instance Name from env", zap.String("instanceName", instanceName))

	credentialRequest := &dpccvdpb.GetCredentialsRequest{
		ApplicationType: dpccvdpb.ApplicationType_MILVUS,
		CollectionId:    collectionId,
		InstanceName:    instanceName,
		BucketName:      bucketName,
		WriteAccess:     true,
	}

	log.Debug("Credential Request to get Credentials : ", zap.Any("credentialRequest", credentialRequest))

	accessManagerClient := GetInstance(ctx)

	// Call the GetCredentials method
	requestStartTime := time.Now().UTC()
	credentialResponse, err := accessManagerClient.Client.GetCredentials(ctx, credentialRequest)
	if err != nil {
		log.Error("Failed to get credentials: %v", zap.Any("error", err))
		return "", "", "", "", "", err
	}
	timeTakenInMilliSeconds := time.Now().UTC().Sub(requestStartTime).Milliseconds()
	log.Info("Time taken to fetch credentials", zap.Int64("timeTakenInMilliSeconds", timeTakenInMilliSeconds))

	log.Debug("Credential Response from access manager : ", zap.Any("credentialResponse", credentialResponse))
	return credentialResponse.GetAccessKeyId(), credentialResponse.GetSecretAccessKey(),
		credentialResponse.GetSessionToken(), credentialResponse.GetTenantKeyId(), credentialResponse.GetExpirationTimestamp(), nil
}

func GetGlobalCredentials(ctx context.Context, bucketName string) (string, string, string, string, error) {

	credentialRequest := &dpccvdpb.GetCredentialsRequest{
		ApplicationType: dpccvdpb.ApplicationType_MILVUS,
		BucketName:      bucketName,
	}

	log.Debug("Global Credential Request to get Credentials : ", zap.Any("credentialRequest", credentialRequest))

	accessManagerClient := GetInstance(ctx)

	// Call the GetCredentials method
	requestStartTime := time.Now().UTC()
	credentialResponse, err := accessManagerClient.Client.GetCredentials(ctx, credentialRequest)
	if err != nil {
		log.Error("Failed to get global credentials: %v", zap.Any("error", err))
		return "", "", "", "", err
	}
	timeTakenInMilliSeconds := time.Now().UTC().Sub(requestStartTime).Milliseconds()
	log.Info("Time taken to fetch global credentials", zap.Int64("timeTakenInMilliSeconds", timeTakenInMilliSeconds))

	log.Debug("Global Credential Response from access manager : ", zap.Any("credentialResponse", credentialResponse))
	return credentialResponse.GetAccessKeyId(), credentialResponse.GetSecretAccessKey(), credentialResponse.GetSessionToken(),
		credentialResponse.ExpirationTimestamp, nil
}

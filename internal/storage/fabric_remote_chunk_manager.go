package storage

import (
	"context"
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/accessmanager"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TransientFabricRemoteChunkManager struct {
	chunkManager        *RemoteChunkManager
	expirationTimestamp string
}

type FabricRemoteChunkManager struct {
	*RemoteChunkManager
	globalTransientRemoteChunkManager *TransientFabricRemoteChunkManager
	chunkManagers                     map[int64]*TransientFabricRemoteChunkManager
	config                            *config
	chunkManagerMutex                 sync.Mutex
	globalChunkManagerMutex           sync.Mutex
}

var _ ChunkManager = (*FabricRemoteChunkManager)(nil)

var Params *paramtable.ComponentParam = paramtable.Get()

func NewFabricRemoteChunkManager(ctx context.Context, c *config) (*FabricRemoteChunkManager, error) {
	log.Info("Creating new Fabric Chunk Manager")

	globalTransientRemoteChunkManager, err := initGlobalChunkManager(ctx, c)
	if err != nil {
		return nil, err
	}

	chunkManager := &FabricRemoteChunkManager{
		RemoteChunkManager:                globalTransientRemoteChunkManager.chunkManager,
		globalTransientRemoteChunkManager: globalTransientRemoteChunkManager,
		chunkManagers:                     map[int64]*TransientFabricRemoteChunkManager{},
		config:                            c,
	}
	return chunkManager, nil
}

func (mcm *FabricRemoteChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
	log.Debug("MultiWrite called ")
	var el error
	for key, value := range kvs {
		log.Info("Data to be written to ", zap.String("filepath", key))
		collID, err := mcm.retrieveCollectionIDFromFilepath(key)
		rcm, err := mcm.getChunkManager(ctx, collID)

		// if chunk manager is available
		if err == nil {
			err = rcm.Write(ctx, key, value)
		} else {
			el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
		}

		// if write  operation failed
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", key))
		}
	}
	return el
}

func (mcm *FabricRemoteChunkManager) Write(ctx context.Context, filePath string, content []byte) error {

	log.Debug("Write called for path ", zap.String("filePath", filePath))

	var el error
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	rcm, err := mcm.getChunkManager(ctx, collID)
	// if chunk manager is available
	if err == nil {
		err = rcm.Write(ctx, filePath, content)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if write  operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", filePath))
	}

	return el
}

func (mcm *FabricRemoteChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {

	log.Debug("Read called for path ", zap.String("filePath", filePath))

	var el error
	log.Info("Data to be read from ", zap.String("filepath", filePath))
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	rcm, err := mcm.getChunkManager(ctx, collID)

	var objectValue []byte
	// if chunk manager is available
	if err == nil {
		objectValue, err = rcm.Read(ctx, filePath)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if read  operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", filePath))
	}

	return objectValue, el
}

func (mcm *FabricRemoteChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {

	log.Debug("MultiRead called for path ", zap.Strings("keys", keys))

	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		log.Info("Data to be written to ", zap.String("filepath", key))
		collID, err := mcm.retrieveCollectionIDFromFilepath(key)
		rcm, err := mcm.getChunkManager(ctx, collID)

		var objectValue []byte
		// if chunk manager is available
		if err == nil {
			objectValue, err = rcm.Read(ctx, key)
		} else {
			el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
		}

		// if read operation failed
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (mcm *FabricRemoteChunkManager) retrieveCollectionIDFromFilepath(key string) (int64, error) {
	return strconv.ParseInt(strings.Split(key, "/")[2], 10, 64)
}

// Remove deletes an object with @key.
func (mcm *FabricRemoteChunkManager) Remove(ctx context.Context, filePath string) error {
	log.Debug("Remove called for path ", zap.String("filePath", filePath))
	err := mcm.removeObject(ctx, mcm.bucketName, filePath)
	if err != nil {
		log.Warn("failed to remove object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *FabricRemoteChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {

	log.Debug("RemoveWithPrefix called for path ", zap.String("prefix", prefix))

	var el error
	// list for any path can be done via global chunk manager
	gcm, err := mcm.getGlobalChunkManager(ctx)

	var removeKeys []string
	// if global chunk manager is available
	if err == nil {
		removeKeys, _, err = gcm.chunkManager.listObjects(ctx, mcm.bucketName, prefix, true)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get global chunkmanager "))
	}

	// if listWithPrefix operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to perform listObjects with prefix %s", prefix))
		return err
	}

	i := 0
	maxGoroutine := 10
	for i < len(removeKeys) {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && i < len(removeKeys); j++ {
			key := removeKeys[i]
			runningGroup.Go(func() error {
				err := mcm.removeObject(groupCtx, mcm.bucketName, key)
				if err != nil {
					log.Warn("failed to remove object", zap.String("path", key), zap.Error(err))
					return err
				}
				return nil
			})
			i++
		}
		if err := runningGroup.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (mcm *FabricRemoteChunkManager) removeObject(ctx context.Context, bucketName, objectName string) error {
	log.Debug("removeObject called for path ", zap.String("objectName", objectName))
	var el error
	collID, err := mcm.retrieveCollectionIDFromFilepath(objectName)
	rcm, err := mcm.getChunkManager(ctx, collID)

	// if chunk manager is available
	if err == nil {
		err = rcm.removeObject(ctx, bucketName, objectName)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if read operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to remove object %s", objectName))
	}
	return el
}

func (mcm *FabricRemoteChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	log.Debug("ListWithPrefix called for path ", zap.String("prefix", prefix))
	var el error
	//always use global chunk manager
	gcm, err := mcm.getGlobalChunkManager(ctx)

	var objectList []string
	var times []time.Time
	// if global chunk manager is available
	if err == nil {
		objectList, times, err = gcm.chunkManager.listObjects(ctx, mcm.bucketName, prefix, recursive)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get global chunkmanager "))
	}

	// if listWithPrefix operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to perform list with prefix %s", prefix))
	}

	return objectList, times, el
}

func initGlobalChunkManager(ctx context.Context, c *config) (*TransientFabricRemoteChunkManager, error) {

	log.Debug("Initializing global chunk manager")

	//TODO add retries
	accessKeyId, secretAccessKey, sessionToken, expirationTimestamp, err := accessmanager.GetGlobalCredentials(
		ctx,
		c.bucketName,
	)

	if err != nil {
		return nil, err
	}

	// cloned the config to be used for this new chunk manager object
	newConfig := c.Clone()

	newConfig.accessKeyID = accessKeyId
	newConfig.secretAccessKeyID = secretAccessKey
	newConfig.sessionToken = sessionToken

	remoteChunkManager, _ := NewRemoteChunkManager(ctx, newConfig)

	transientChunkManager := &TransientFabricRemoteChunkManager{
		remoteChunkManager,
		expirationTimestamp,
	}

	return transientChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) getGlobalChunkManager(ctx context.Context) (*TransientFabricRemoteChunkManager, error) {
	if mcm.globalTransientRemoteChunkManager.chunkManager == nil || mcm.isChunkManagerExpired(mcm.globalTransientRemoteChunkManager) {
		mcm.globalChunkManagerMutex.Lock()
		defer mcm.globalChunkManagerMutex.Unlock()

		if mcm.globalTransientRemoteChunkManager.chunkManager == nil || mcm.isChunkManagerExpired(mcm.globalTransientRemoteChunkManager) {
			transientChunkManager, err := initGlobalChunkManager(ctx, mcm.config)
			if err != nil {
				log.Error("Error occurred while trying to initialize global chunk manager")
				return nil, err
			}
			mcm.globalTransientRemoteChunkManager = transientChunkManager
		}

	}
	return mcm.globalTransientRemoteChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) getChunkManager(ctx context.Context, collID int64) (*RemoteChunkManager, error) {
	log.Info("getting chunk manager for collection id", zap.Int64("collID", collID))
	var err error
	if !mcm.isValidChunkManagerPresent(collID) {
		err = mcm.upsertChunkManager(ctx, collID)
	}
	if err != nil {
		log.Error("Error while loading chunk manager for collection id", zap.Int64("collectionId", collID))
		// TODO decide to return chunk manager with stale credentials if present
		return nil, err
	}
	return mcm.chunkManagers[collID].chunkManager, nil
}

/*
This check returns true only if there is a chunk manager in the map and the credentials for that chunk manager
don't need to be refreshed yet
*/
func (mcm *FabricRemoteChunkManager) isValidChunkManagerPresent(collID int64) bool {
	transientChunkManager, ok := mcm.chunkManagers[collID]
	if ok {
		// check if the chunk manager needs to be refreshed
		log.Debug("Checking if the chunk manager expired for collection id", zap.Int64("collectionId", collID))
		validChunkManagerPresent := !mcm.isChunkManagerExpired(transientChunkManager)
		log.Debug("Chunk manager state for collection id", zap.Bool("validChunkManagerPresent", validChunkManagerPresent))

		return validChunkManagerPresent
	}
	return false
}

func (mcm *FabricRemoteChunkManager) isChunkManagerExpired(transientChunkManager *TransientFabricRemoteChunkManager) bool {
	// check if the chunk manager needs to be refreshed
	currentTime := time.Now().UTC()
	expirationTime, err := time.Parse(time.RFC3339, transientChunkManager.expirationTimestamp)
	if err != nil {
		log.Error("Error parsing time string for the current chunk manager", zap.String("expirationTimeStamp", transientChunkManager.expirationTimestamp))
		return true
	}

	credentialsRefreshThreshold := Params.CommonCfg.CredentialsRefreshThresholdMinutes.GetAsFloat()

	remainingValidity := expirationTime.Sub(currentTime)

	log.Debug("chunkManager expiration details",
		zap.Float64("credentialsRefreshThreshold", credentialsRefreshThreshold),
		zap.Time("currentTime", currentTime),
		zap.Time("expirationTime", expirationTime),
		zap.Float64("remainingValidity", remainingValidity.Minutes()),
	)

	if remainingValidity.Minutes() < credentialsRefreshThreshold {
		log.Debug("Chunk Manager needs to be refreshed")
		return true
	} else {
		log.Debug("Chunk Manager does not need to be refreshed")
		return false
	}
}

/*
This method adds a new entry in the chunk manager map if there is none for the given collection id
If the collection id chunk manager exists then it updates the chunk manager with latest credentials from access manager
*/
func (mcm *FabricRemoteChunkManager) upsertChunkManager(ctx context.Context, collID int64) error {

	mcm.chunkManagerMutex.Lock()
	defer mcm.chunkManagerMutex.Unlock()
	log.Debug("Entered lock to initialize chunk manager for collection id : ", zap.Int64("collectionId", collID))
	// double check to ensure chunk manager is not initialized twice
	if !mcm.isValidChunkManagerPresent(collID) {
		log.Info("Initializing chunk manager for collection id : ", zap.Int64("collectionId", collID))

		//TODO add retries
		accessKeyId, secretAccessKey, sessionToken, tenantKeyId, expirationTimestamp, err := accessmanager.GetCredentialsForCollection(
			ctx,
			fmt.Sprintf("%d", collID),
			mcm.config.bucketName,
		)

		if err != nil {
			return err
		}

		// cloned the config to be used for this new chunk manager object
		newConfig := mcm.config.Clone()

		newConfig.accessKeyID = accessKeyId
		newConfig.secretAccessKeyID = secretAccessKey
		newConfig.sessionToken = sessionToken
		newConfig.sseKms = tenantKeyId

		remoteChunkManager, _ := NewRemoteChunkManager(ctx, newConfig)

		transientChunkManager := &TransientFabricRemoteChunkManager{
			remoteChunkManager,
			expirationTimestamp,
		}

		mcm.chunkManagers[collID] = transientChunkManager
	}
	return nil
}

package storage

import (
	"context"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type FabricChunkManagerFactory struct {
	chunkManagerFactory *ChunkManagerFactory
}

func (f *FabricChunkManagerFactory) newChunkManager(ctx context.Context, engine string) (ChunkManager, error) {
	factory := f.chunkManagerFactory
	c := factory.config
	return NewFabricRemoteChunkManager(ctx, c)
}

func (f *FabricChunkManagerFactory) NewPersistentStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return f.newChunkManager(ctx, f.chunkManagerFactory.persistentStorage)
}

func NewFabricChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *FabricChunkManagerFactory {
	chunkManagerFactory := NewChunkManagerFactoryWithParam(params)
	f := &FabricChunkManagerFactory{chunkManagerFactory: chunkManagerFactory}
	return f
}

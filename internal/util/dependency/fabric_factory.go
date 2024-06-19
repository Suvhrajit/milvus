package dependency

import (
	"context"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type FabricFactory struct {
	DefaultFactory
}

func (f *FabricFactory) Init(params *paramtable.ComponentParam) {
	f.DefaultFactory.Init(params)
	f.chunkManagerFactory = storage.NewFabricChunkManagerFactoryWithParam(params)
}

func (f *FabricFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

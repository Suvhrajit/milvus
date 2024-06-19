package storage

import (
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// Option for setting params used by chunk manager client.
type config struct {
	address           string
	bucketName        string
	accessKeyID       string
	secretAccessKeyID string
	useSSL            bool
	sslCACert         string
	createBucket      bool
	rootPath          string
	useIAM            bool
	cloudProvider     string
	iamEndpoint       string
	useVirtualHost    bool
	region            string
	requestTimeoutMs  int64
	sseKms            string
	sessionToken      string
}

func newDefaultConfig() *config {
	return &config{}
}

func (c *config) Clone() *config {
	return &config{
		address:           c.address,
		bucketName:        c.bucketName,
		accessKeyID:       c.accessKeyID,
		secretAccessKeyID: c.secretAccessKeyID,
		useSSL:            c.useSSL,
		sslCACert:         c.sslCACert,
		createBucket:      c.createBucket,
		rootPath:          c.rootPath,
		useIAM:            c.useIAM,
		cloudProvider:     c.cloudProvider,
		iamEndpoint:       c.iamEndpoint,
		useVirtualHost:    c.useVirtualHost,
		region:            c.region,
		requestTimeoutMs:  c.requestTimeoutMs,
		sseKms:            c.sseKms,
		sessionToken:      c.sessionToken,
	}
}

// Option is used to config the retry function.
type Option func(*config)

func Address(addr string) Option {
	return func(c *config) {
		c.address = addr
	}
}

func BucketName(bucketName string) Option {
	return func(c *config) {
		c.bucketName = bucketName
	}
}

func AccessKeyID(accessKeyID string) Option {
	return func(c *config) {
		c.accessKeyID = accessKeyID
	}
}

func SecretAccessKeyID(secretAccessKeyID string) Option {
	return func(c *config) {
		c.secretAccessKeyID = secretAccessKeyID
	}
}

func UseSSL(useSSL bool) Option {
	return func(c *config) {
		c.useSSL = useSSL
	}
}

func SslCACert(sslCACert string) Option {
	return func(c *config) {
		c.sslCACert = sslCACert
	}
}

func CreateBucket(createBucket bool) Option {
	return func(c *config) {
		c.createBucket = createBucket
	}
}

func RootPath(rootPath string) Option {
	log.Info("rootPath", zap.String("rootPath", rootPath))
	return func(c *config) {
		c.rootPath = rootPath
	}
}

func UseIAM(useIAM bool) Option {
	return func(c *config) {
		c.useIAM = useIAM
	}
}

func CloudProvider(cloudProvider string) Option {
	return func(c *config) {
		c.cloudProvider = cloudProvider
	}
}

func IAMEndpoint(iamEndpoint string) Option {
	return func(c *config) {
		c.iamEndpoint = iamEndpoint
	}
}

func UseVirtualHost(useVirtualHost bool) Option {
	return func(c *config) {
		c.useVirtualHost = useVirtualHost
	}
}

func Region(region string) Option {
	return func(c *config) {
		c.region = region
	}
}

func RequestTimeout(requestTimeoutMs int64) Option {
	return func(c *config) {
		c.requestTimeoutMs = requestTimeoutMs
	}
}

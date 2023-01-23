package dyngeo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/geo/s2"
)

type GeoOptions func(c *Config)

// WithRetryOnFilterOfRadiusQuery sets a max number of retries allowed for radius query;
// Important to mention, this retry will be applied when the algo is trying to find if the results from dynamo db
// are part of the created circle. When this is enabled, the algorithm will do
//
//	radius = radius * multiplier; for each reach the maxRetries - do radius * multiplier
//
// be aware that multipler has set 2 as default
func WithRetryOnFilterOfRadiusQuery(maxRetries, multiplier int) GeoOptions {
	return func(c *Config) {
		c.filterByRadiusMaxRetries = maxRetries
		c.filterByRadiusRetryMultiplier = 2
		if multiplier > 0 {
			c.filterByRadiusRetryMultiplier = multiplier
		}
	}
}

// WithLongitudeFirst sets the order of a geo coordinate to be readed, the default value is false.
func WithLongitudeFirst(f bool) GeoOptions {
	return func(c *Config) {
		c.longitudeFirst = f
	}
}

func WithUnmarshalAsJSON(f bool) GeoOptions {
	return func(c *Config) {
		c.unmarshalAsJSON = f
	}
}

func WithConsistentRead(consistentRead bool) GeoOptions {
	return func(c *Config) {
		c.consistentRead = consistentRead
	}
}

// WithTableName sets the table name expected to be queried
func WithTableName(name string) GeoOptions {
	return func(c *Config) {
		c.tableName = name
	}
}

// WithHashKey sets a custom hashKey attribute name. When it's missing, the value is `hashKey`
func WithHashkey(attributeName string) GeoOptions {
	return func(c *Config) {
		c.hashKeyAttributeName = attributeName
	}
}

// WithHashKey sets a custom rangeKey attribute name. When it's missing, the value is `rangeKey`
func WithRangekey(attributeName string) GeoOptions {
	return func(c *Config) {
		c.rangeKeyAttributeName = attributeName
	}
}

// WithGeoHashkey sets a custom geo hashKey attribute name. When it's missing, the value is `geohash`
func WithGeoHashkey(attributeName string) GeoOptions {
	return func(c *Config) {
		c.geoHashAttributeName = attributeName
	}
}

// WithGeoJSONKey sets a custom geo JSON key attribute name. When it's missing, the value is `geoJson`
func WithGeoJSONKey(attributeName string) GeoOptions {
	return func(c *Config) {
		c.geoJSONAttributeName = attributeName
	}
}

// WithGeoIndex sets a custom geo index name. When it's missing, the value is `geohash-index`
func WithGeoIndex(name string) GeoOptions {
	return func(c *Config) {
		c.geoHashIndexName = name
	}
}

// WithDynamoClient sets the dynamo client to be used
func WithDynamoClient(client *dynamodb.Client) GeoOptions {
	return func(c *Config) {
		c.dynamoDBClient = client
	}
}

// WithS2RegionCoverer sets the s2.RegionCoverer to be used
func WithS2RegionCoverer(s2rc s2.RegionCoverer) GeoOptions {
	return func(c *Config) {
		c.s2RegionCoverer = s2rc
	}
}

// WithHashkeyLen sets a custom hashKey len. When it's missing, the value is `5`
func WithHashkeyLen(len int8) GeoOptions {
	return func(c *Config) {
		c.hashKeyLength = len
	}
}

// makeDefaultConfig sets config with default values
func makeDefaultConfig() *Config {
	return &Config{
		hashKeyAttributeName:  "hashKey",
		rangeKeyAttributeName: "rangeKey",
		geoHashAttributeName:  "geohash",
		geoJSONAttributeName:  "geoJson",
		geoHashIndexName:      "geohash-index",
		hashKeyLength:         5,
		s2RegionCoverer: s2.RegionCoverer{
			MinLevel: 10,
			MaxLevel: 10,
			MaxCells: 10,
			LevelMod: 0,
		},
	}
}

type Config struct {
	filterByRadiusMaxRetries      int
	filterByRadiusRetryMultiplier int
	longitudeFirst                bool
	unmarshalAsJSON               bool
	consistentRead                bool
	hashKeyLength                 int8
	tableName                     string
	hashKeyAttributeName          string
	rangeKeyAttributeName         string
	geoHashAttributeName          string
	geoJSONAttributeName          string
	geoHashIndexName              string
	dynamoDBClient                *dynamodb.Client
	s2RegionCoverer               s2.RegionCoverer
}

func MakeConfig(opts ...GeoOptions) *Config {
	cfg := makeDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

func (cfg Config) Valid() error {
	if cfg.dynamoDBClient == nil {
		return ErrMissingDynamoClient{}
	}

	if cfg.tableName == "" {
		return ErrMissingDynamoTableName{}
	}

	return nil
}

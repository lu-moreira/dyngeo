package dyngeo

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/geo/s2"
)

type GeoHashToLastEvaluatedDBValue map[uint64]map[string]types.AttributeValue

type queryResponse []map[string]types.AttributeValue

func (r queryResponse) Unmarshal(asJSON bool, out interface{}) error {
	err := attributevalue.UnmarshalListOfMapsWithOptions(r, out, func(options *attributevalue.DecoderOptions) {
		if asJSON {
			options.TagKey = "json"
		}
	})
	if err != nil {
		return err
	}

	return nil
}

type querier struct {
	longitudeFirst                bool
	unmarshalAsJSON               bool
	hashKeyLen                    int8
	filterByRadiusRetryMultiplier int
	filterByRadiusMaxRetries      int
	geoJSONAttributeName          string
	s2rc                          s2.RegionCoverer
	db                            db
}

func newQuerier(cfg Config, db db) querier {
	return querier{
		filterByRadiusRetryMultiplier: cfg.filterByRadiusRetryMultiplier,
		filterByRadiusMaxRetries:      cfg.filterByRadiusMaxRetries,
		longitudeFirst:                cfg.longitudeFirst,
		unmarshalAsJSON:               cfg.unmarshalAsJSON,
		hashKeyLen:                    cfg.hashKeyLength,
		geoJSONAttributeName:          cfg.geoJSONAttributeName,
		s2rc:                          cfg.s2RegionCoverer,
		db:                            db,
	}
}

func (q querier) QueryRadiusWithPagination(ctx context.Context, input QueryRadiusInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint) ([]map[string]types.AttributeValue, GeoHashToLastEvaluatedDBValue, error) {
	latLngRect := boundingLatLngFromQueryRadiusInput(input)
	covering := newCovering(q.s2rc.Covering(s2.Region(latLngRect)))
	results, newHashToLEntry := q.dispatchQueriesWithPagination(ctx, covering, input.GeoQueryInput, hashToLastEvaluatedEntry, limit)
	filteredEntries, err := q.filterByRadius(results, input)
	return filteredEntries, newHashToLEntry, err
}

func (q querier) QueryRectangle(ctx context.Context, input QueryRectangleInput) ([]map[string]types.AttributeValue, error) {
	latLngRect := rectFromQueryRectangleInput(input)
	covering := newCovering(q.s2rc.Covering(s2.Region(latLngRect)))
	results := q.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return q.filterByRect(results, input)
}

func (q querier) QueryRadius(ctx context.Context, input QueryRadiusInput) ([]map[string]types.AttributeValue, error) {
	latLngRect := boundingLatLngFromQueryRadiusInput(input)
	covering := newCovering(q.s2rc.Covering(s2.Region(latLngRect)))
	results := q.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return q.filterByRadius(results, input)
}

func (q querier) dispatchQueriesWithPagination(ctx context.Context, covering covering, input GeoQueryInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint) ([]map[string]types.AttributeValue, GeoHashToLastEvaluatedDBValue) {
	var results [][]*dynamodb.QueryOutput
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}

	hashRanges := covering.getGeoHashRanges(q.hashKeyLen)
	iterations := len(hashRanges)
	newLastEvaluatedEntries := make(GeoHashToLastEvaluatedDBValue)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int, queryInput dynamodb.QueryInput) {
			defer wg.Done()
			g := hashRanges[i]
			hashKey := generateHashKey(g.rangeMin, q.hashKeyLen)
			// look into the map to check if there has been a query for this hash before
			// if there was - reuse the last evaluated key for the hash
			if hashToLastEvaluatedEntry != nil {
				if lastEvalKey, ok := hashToLastEvaluatedEntry[hashKey]; ok {
					// this will only be true when there is in fact no more entries to be processed
					if lastEvalKey == nil {
						return
					}
					queryInput.ExclusiveStartKey = lastEvalKey
				}
			}
			// query hash and stop if reach the limit
			output := q.db.queryGeoHash(ctx, queryInput, hashKey, g, int(limit))
			if len(output) > 0 {
				mtx.Lock()
				newLastEvaluatedEntries[hashKey] = output[len(output)-1].LastEvaluatedKey
				results = append(results, output)
				mtx.Unlock()
			}
		}(i, input.QueryInput)
	}

	wg.Wait()

	var mergedResults []map[string]types.AttributeValue
	for _, o := range results {
		for _, it := range o {
			mergedResults = append(mergedResults, it.Items...)
		}
	}
	return mergedResults, newLastEvaluatedEntries
}

func (q querier) dispatchQueries(ctx context.Context, covering covering, input GeoQueryInput) []map[string]types.AttributeValue {
	results := [][]*dynamodb.QueryOutput{}
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}

	hashRanges := covering.getGeoHashRanges(q.hashKeyLen)
	iterations := len(hashRanges)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()
			g := hashRanges[i]
			hashKey := generateHashKey(g.rangeMin, q.hashKeyLen)
			output := q.db.queryGeoHash(ctx, input.QueryInput, hashKey, g, -1)
			mtx.Lock()
			results = append(results, output)
			mtx.Unlock()
		}(i)
	}

	wg.Wait()

	var mergedResults []map[string]types.AttributeValue
	for _, o := range results {
		for _, it := range o {
			mergedResults = append(mergedResults, it.Items...)
		}
	}

	return mergedResults
}

func (q querier) filterByRect(list []map[string]types.AttributeValue, input QueryRectangleInput) ([]map[string]types.AttributeValue, error) {
	var filtered []map[string]types.AttributeValue
	latLngRect := rectFromQueryRectangleInput(input)

	for _, item := range list {
		latLng, err := q.latLngFromItem(item)
		if err != nil {
			return nil, err
		}

		if latLngRect.ContainsLatLng(*latLng) {
			filtered = append(filtered, item)
		}
	}

	return filtered, nil
}

func (q querier) filterByRadius(list []map[string]types.AttributeValue, input QueryRadiusInput) ([]map[string]types.AttributeValue, error) {
	var filtered []map[string]types.AttributeValue

	centerLatLng := s2.LatLngFromDegrees(input.CenterPoint.Latitude, input.CenterPoint.Longitude)

	for _, item := range list {
		radius := input.RadiusInMeter
		latLng, err := q.latLngFromItem(item)
		if err != nil {
			return nil, err
		}

		// do
		if getEarthDistance(centerLatLng, *latLng) <= float64(radius) {
			filtered = append(filtered, item)
			continue
		}

		// while
		maxRetries := q.filterByRadiusMaxRetries
		for i := 1; i <= maxRetries; i++ {
			radius = radius * q.filterByRadiusRetryMultiplier
			if getEarthDistance(centerLatLng, *latLng) <= float64(radius) {
				filtered = append(filtered, item)
				break
			}
			maxRetries--
		}

	}

	return filtered, nil
}

func (q querier) latLngFromItem(item map[string]types.AttributeValue) (*s2.LatLng, error) {
	switch geo := item[q.geoJSONAttributeName].(type) {
	case *types.AttributeValueMemberB:
		geoJSONAttr := GeoJSONAttribute{}
		err := json.Unmarshal(geo.Value, &geoJSONAttr)
		if err != nil {
			return nil, err
		}

		coordinates := geoJSONAttr.Coordinates
		var lng float64
		var lat float64
		if q.longitudeFirst {
			lng = coordinates[0]
			lat = coordinates[1]
		} else {
			lng = coordinates[1]
			lat = coordinates[0]
		}

		latLng := s2.LatLngFromDegrees(lat, lng)
		return &latLng, nil
	}

	return nil, ErrFailedToConvertItemToGeoPoint{name: q.geoJSONAttributeName}
}

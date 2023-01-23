package dyngeo

import (
	"context"
)

type Geo struct {
	db              db
	q               querier
	unmarshalAsJSON bool
}

func New(cfg *Config) (*Geo, error) {
	if err := cfg.Valid(); err != nil {
		return nil, err
	}

	db := newDB(*cfg)

	g := &Geo{
		unmarshalAsJSON: cfg.unmarshalAsJSON,
		db:              db,
		q:               newQuerier(*cfg, db),
	}

	return g, nil
}

func (g Geo) GetPoint(ctx context.Context, input GetPointInput) (*GetPointOutput, error) {
	return g.db.getPoint(ctx, input)
}

func (g Geo) QueryRadius(ctx context.Context, input QueryRadiusInput, out interface{}) error {
	output, err := g.q.QueryRadius(ctx, input)
	if err != nil {
		return err
	}

	return queryResponse(output).Unmarshal(g.unmarshalAsJSON, out)
}

func (g Geo) QueryRectangle(ctx context.Context, input QueryRectangleInput, out interface{}) error {
	output, err := g.q.QueryRectangle(ctx, input)
	if err != nil {
		return err
	}

	return queryResponse(output).Unmarshal(g.unmarshalAsJSON, out)
}

func (g Geo) QueryRadiusPaginated(ctx context.Context, input QueryRadiusInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint, out interface{}) (GeoHashToLastEvaluatedDBValue, error) {
	if limit == 0 {
		return nil, ErrPaginatedQueryInvalidLimit{}
	}
	output, newHashToLEntry, err := g.q.QueryRadiusWithPagination(ctx, input, hashToLastEvaluatedEntry, limit)
	if err != nil {
		return nil, err
	}
	return newHashToLEntry, queryResponse(output).Unmarshal(g.unmarshalAsJSON, out)
}

func (g Geo) PutPoint(ctx context.Context, input PutPointInput) (*PutPointOutput, error) {
	return g.db.putPoint(ctx, input)
}

func (g Geo) BatchWritePoints(ctx context.Context, inputs []PutPointInput) (*BatchWritePointOutput, error) {
	return g.db.batchWritePoints(ctx, inputs)
}

func (g Geo) UpdatePoint(ctx context.Context, input UpdatePointInput) (*UpdatePointOutput, error) {
	return g.db.updatePoint(ctx, input)
}

func (g Geo) DeletePoint(ctx context.Context, input DeletePointInput) (*DeletePointOutput, error) {
	return g.db.deletePoint(ctx, input)
}

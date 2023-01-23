package dyngeo

import "fmt"

type ErrMissingDynamoClient struct{}

func (ErrMissingDynamoClient) Error() string {
	return "dynamo client is required"
}

type ErrMissingDynamoTableName struct{}

func (ErrMissingDynamoTableName) Error() string {
	return "table name is required"
}

type ErrPaginatedQueryInvalidLimit struct{}

func (ErrPaginatedQueryInvalidLimit) Error() string {
	return "invalid limit provided"
}

type ErrFailedToConvertItemToGeoPoint struct {
	name string
}

func (e ErrFailedToConvertItemToGeoPoint) Error() string {
	return fmt.Sprintf("invalid item at %s", e.name)
}

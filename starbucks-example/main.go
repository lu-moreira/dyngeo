package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	c "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofrs/uuid"
	"github.com/lu-moreira/dyngeo"
)

var (
	dbClient *dynamodb.Client
	dg       *dyngeo.DynGeo
)

const BATCH_SIZE = 25
const TableName = "coffee-shops"
const DynamoDbURL = "http://localhost:4566"

type Starbucks struct {
	Position Position `json:"position"`
	GeoJSON  []byte   `json:"geoJson"`
	Name     string   `json:"name"`
	Address  string   `json:"address"`
	Phone    string   `json:"phone"`
}

type Position struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
}

func main() {
	awsCfg, err := c.LoadDefaultConfig(context.Background(), func(o *c.LoadOptions) error {
		o.Region = "us-east-1"
		// if u don't wanna use from local or localstack, remove this option
		o.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           DynamoDbURL,
				SigningRegion: region,
			}, nil
		})
		return nil
	})
	if err != nil {
		panic(err)
	}

	dbClient = dynamodb.NewFromConfig(awsCfg)

	dg, err = dyngeo.New(dyngeo.DynGeoConfig{
		DynamoDBClient: dbClient,
		HashKeyLength:  5,
		TableName:      TableName,
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	setupTable(ctx, true)
	loadData(ctx)
	queryData(ctx)
}

func tableExists(ctx context.Context, tableName string) (bool, error) {
	_, err := dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(TableName),
	})
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", TableName)
			err = nil
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", TableName, err)
		}
		return false, err
	}

	return true, err
}

func setupTable(ctx context.Context, mustRecreateTable bool) {
	exists, _ := tableExists(ctx, TableName)
	if exists && mustRecreateTable {
		_, err := dbClient.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(TableName),
		})
		if err != nil {
			panic(err)
		}
	}

	createTableInput := dyngeo.GetCreateTableRequest(dg.Config)
	createTableInput.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(5)
	createTableOutput, err := dbClient.CreateTable(ctx, createTableInput)
	if err != nil {
		panic(err)
	}

	log.Println("table created")
	log.Println(createTableOutput)
}

func loadData(ctx context.Context) {
	f, err := ioutil.ReadFile("starbucks_us_locations.json")
	if err != nil {
		panic(err)
	}
	coffeeShops := []Starbucks{}
	err = json.Unmarshal([]byte(f), &coffeeShops)
	if err != nil {
		panic(err)
	}

	batchInput := []dyngeo.PutPointInput{}
	for _, s := range coffeeShops {
		id, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}

		name, err := attributevalue.Marshal(s.Name)
		if err != nil {
			panic(err)
		}

		address, err := attributevalue.Marshal(s.Address)
		if err != nil {
			panic(err)
		}

		input := dyngeo.PutPointInput{
			PutItemInput: dynamodb.PutItemInput{
				Item: map[string]types.AttributeValue{
					"name":    name,
					"address": address,
				},
			},
		}
		input.RangeKeyValue = id.String()
		input.GeoPoint = dyngeo.GeoPoint{
			Latitude:  s.Position.Latitude,
			Longitude: s.Position.Longitude,
		}
		batchInput = append(batchInput, input)
	}

	batches := [][]dyngeo.PutPointInput{}
	for BATCH_SIZE < len(batchInput) {
		batchInput, batches = batchInput[BATCH_SIZE:], append(batches, batchInput[0:BATCH_SIZE:BATCH_SIZE])
	}
	batches = append(batches, batchInput)

	log.Println("expected total batches of ", len(batches))
	for count, batch := range batches {
		output, err := dg.BatchWritePoints(ctx, batch)
		if err != nil {
			panic(err)
		}
		log.Printf("Batch %d written: %+v\n", count, output)
	}
}

func parseStringSlice(s []float64) []string {
	f := make([]string, len(s))
	for idx := range s {
		f[idx] = fmt.Sprintf("%.5f", s[idx])
	}
	return f
}

func queryData(ctx context.Context) {
	start := time.Now()
	sbs := []Starbucks{}

	err := dg.QueryRadius(ctx, dyngeo.QueryRadiusInput{
		CenterPoint: dyngeo.GeoPoint{
			Latitude:  40.7769099,
			Longitude: -73.9822532,
		},
		RadiusInMeter: 1000,
	}, &sbs)
	if err != nil {
		panic(err)
	}

	log.Println("Total results ", len(sbs))

	ss := strings.Builder{}
	for _, sb := range sbs {
		ss.WriteString("---\n")
		ss.WriteString(fmt.Sprintf("Name: %s\n", sb.Name))
		ss.WriteString(fmt.Sprintf("Phone: %s\n", sb.Phone))
		ss.WriteString(fmt.Sprintf("Address: %s\n", sb.Address))

		geoJSONAttr := dyngeo.GeoJSONAttribute{}
		_ = json.Unmarshal(sb.GeoJSON, &geoJSONAttr)
		ss.WriteString(fmt.Sprintf("Location: %s - [%s]\n", geoJSONAttr.Type, strings.Join(parseStringSlice(geoJSONAttr.Coordinates), ",")))

		log.Println(ss.String())
		ss.Reset()
	}

	log.Print("Executed in: ")
	log.Println(time.Since(start))
}

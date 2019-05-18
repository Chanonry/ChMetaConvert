// Package ChMetaConvert : converts the CHMetaTest table document format to a new List Item format table
package ChMetaConvert

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	DbHelpers "github.com/Chanonry/dbhelpers"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/cenkalti/backoff"
)

const version = "1.2.0" //concurrent

// synchronous
// performance:
// total 6.255s for 1 company 23 document records
// UnMarshal 213 ms
// Marshal 22 µs
// PutItem 200ms x 23
// my internet

// concurrent
// paginate table - 23 ms per item or 8ms concurrently
// with 4 goroutines - marshalling tales about 590µs per accounts item

const testPages int = 2     // testing parameter
const scanners int64 = 2    // number of table scan goroutines
const marshallers int64 = 4 // number of marshaller goroutines
const batchSize int = 25    // batch size for BatchWrite

var scanner int64
var scannerWG sync.WaitGroup
var marshaller int64
var marshallerWG sync.WaitGroup

var dbsvc, _ = DbHelpers.NewService()

var chMetaTable = DbHelpers.DynamoTable{
	TableName:  "CHMetaTest",
	PrimaryKey: "Number",
	SearchKey:  "",
	Svc:        dbsvc,
}

type table DbHelpers.DynamoTable

var acctsMetaData = table{
	TableName:  "AcctsMetaData",
	PrimaryKey: "CoNum",
	SearchKey:  "MUDstamp",
	Svc:        dbsvc,
}

type docMetaData struct {
	contentFile string
	description string
	docURL      string
	madeUpDate  string
}

type cHMetaItem struct {
	name     string
	number   string
	accounts []docMetaData
}

func paginateTablePool(tb *DbHelpers.DynamoTable, c chan map[string]dynamodb.AttributeValue) {

	// profiling:
	// 1081 Items
	// 3 pages
	// 128.5 Capacity units
	// approx total is 2000 capacity units
	// 13 second duration
	// 12 ms per item so much faster I can put items
	// on single pages was taking ~ 20ms per item

	// concurrently does it in about 8ms 1426 items in 11.62secs

	// batch write items of 25 items, 400kb each total 16Mb
	for scanner = 0; scanner < scanners; scanner++ {
		scannerWG.Add(1)
		go func(sc, scs int64) {

			gor := strconv.FormatInt(sc, 10) // str repr of goroutine number
			fmt.Println("Launching Paginator GOR:", gor)

			// initialise variables
			startKey := map[string]dynamodb.AttributeValue{}
			pages := 0
			total := 0
			var totalConsumedCapacity float64
			totalConsumedCapacity = 0.0

			for {
				input := &dynamodb.ScanInput{
					TableName:              aws.String(tb.TableName),
					ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityTotal,
				}

				if len(startKey) != 0 { // no startKey for first loop but add for later pages
					input.ExclusiveStartKey = startKey
				}

				if scs > 1 { // multiple scanning goroutines demanded
					input.Segment = &sc
					input.TotalSegments = &scs
				}

				req := tb.Svc.ScanRequest(input)
				// fmt.Println("Sending request GOR:", gor)
				result, err := req.Send(context.Background())
				if err != nil {
					fmt.Printf("Paginate GOR%v: failed to make Query API call, %v\n", gor, err)
				}

				// put items on the channel
				// fmt.Println("Putting Result on channel GOR:", gor)
				for _, item := range result.Items {
					c <- item
					total++
				}

				if len(result.LastEvaluatedKey) != 0 {
					startKey = result.LastEvaluatedKey
					// fmt.Printf("GOR%v: LastEvaluatedKey%v\n", gor, result.LastEvaluatedKey)
					// fmt.Printf("GOR%v: startKey%v\n", gor, startKey)
					pages++
					fmt.Printf("Paginate GOR:%v Page:%v\n", gor, pages)
					fmt.Printf("Paginate GOR:%v Page:%v TestPages:%v\n", gor, pages, testPages)
					totalConsumedCapacity += *result.ConsumedCapacity.CapacityUnits
				}

				if len(result.LastEvaluatedKey) == 0 || pages == testPages { // no more pages OR test finished
					fmt.Printf("Paginate GOR%v: Processed items count:%v\n", gor, total)
					fmt.Printf("Paginate GOR%v: Processed pages count:%v\n", gor, pages)
					fmt.Printf("Paginate GOR%v: Total Consumed Capacity:%v\n", gor, totalConsumedCapacity)
					fmt.Println("Stopping Paginate GOR:", gor)

					scannerWG.Done()

					return
				}
			}
		}(scanner, scanners)
	}
}

func (tb *table) unMarshallPool(ipCh chan map[string]dynamodb.AttributeValue, opCh chan map[string]dynamodb.AttributeValue) {

	// processing 1 item in ~ 590µs !
	// 19693 items in 11.649s
	for marshaller = 0; marshaller < marshallers; marshaller++ {
		marshallerWG.Add(1)
		go func(ms int64) {
			gor := strconv.FormatInt(ms, 10) // str repr of goroutine number
			fmt.Println("Launching Marshall GOR:", gor)

			for item := range ipCh {
				// fmt.Println("Taking Item of ipCh Marshall GOR:", gor)
				for k, v := range item { // just the Item data no metadata or headers
					var coMeta = cHMetaItem{} //initialise the target struct for Unmarshaling

					switch k {
					case "Name":
						{
							var coName string
							nameAttrVal := v
							_ = dynamodbattribute.Unmarshal(&nameAttrVal, &coName)
							coMeta.name = coName
						}
					case "Number":
						{
							var coNum string
							numAttrVal := v
							_ = dynamodbattribute.Unmarshal(&numAttrVal, &coNum)
							coMeta.number = coNum
						}
					case "Accounts":
						{
							tempDocMeta := docMetaData{}
							for _, allDocs := range v.M { // extract the map for all accounts docs
								for k, v := range allDocs.M { // exptract the map for one accounts doc
									attrPointer := v
									temp := ""
									err := dynamodbattribute.Unmarshal(&attrPointer, &temp)
									if err != nil {
										fmt.Println("Marshalling error:", err)
									}

									switch k {
									case "DocURL":
										{
											tempDocMeta.docURL = temp
										}
									case "ContentFile":
										{
											tempDocMeta.contentFile = temp
										}
									case "MadeUpDate":
										{
											tempDocMeta.madeUpDate = temp
										}
									case "Description":
										{
											tempDocMeta.description = temp
										}
									}
								}
								coMeta.accounts = append(coMeta.accounts, tempDocMeta)
							}
						}
					}
					for _, acct := range coMeta.accounts { // marshall into item with scalar attributes

						attrValMap := make(map[string]dynamodb.AttributeValue)

						valNUM, err := dynamodbattribute.Marshal(coMeta.number)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap[tb.PrimaryKey] = *valNUM

						valMUD, err := dynamodbattribute.Marshal(acct.madeUpDate)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap[tb.SearchKey] = *valMUD

						valNAME, err := dynamodbattribute.Marshal(coMeta.name)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap["name"] = *valNAME

						valFILE, err := dynamodbattribute.Marshal(acct.contentFile)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap["contentFile"] = *valFILE

						valDESC, err := dynamodbattribute.Marshal(acct.description)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap["description"] = *valDESC

						valURL, err := dynamodbattribute.Marshal(acct.docURL)
						if err != nil {
							fmt.Println("error marshalling")
						}
						attrValMap["docURL"] = *valURL

						// fmt.Println("Putting map onto chan Marshall GOR:", gor)
						opCh <- attrValMap
					}
				}
			}
			fmt.Println("Marshall goroutine finished :", gor)
			marshallerWG.Done()
		}(marshaller)
	}
	return
}

// batchWritePool : creates a pool of workers to perform BatchWrites to dynamodb table
func (tb *table) batchWritePool(ipCh chan map[string]dynamodb.AttributeValue) {

	for marshaller = 0; marshaller < marshallers; marshaller++ {
		marshallerWG.Add(1)

		go func(ms int64) {
			writeRequestS := []dynamodb.WriteRequest{}
			count := 0

			for attrValMap := range ipCh {
				count++
				writeRequest := dynamodb.WriteRequest{ // build the write request input
					PutRequest: &dynamodb.PutRequest{ // one item
						Item: attrValMap,
					},
				}
				writeRequestS = append(writeRequestS, writeRequest) // append to the xs

				if count == batchSize { // write the batch
					BatchInput := dynamodb.BatchWriteItemInput{
						RequestItems: map[string][]dynamodb.WriteRequest{
							tb.TableName: writeRequestS},
					}
					tb.batchWriteRetry(BatchInput)
					writeRequestS = []dynamodb.WriteRequest{}

				}
			} // range on channel loop

			// channel closed - flush any remaining PutRequests
			BatchInput := dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodb.WriteRequest{
					tb.TableName: writeRequestS},
			}
			tb.batchWriteRetry(BatchInput)

			marshallerWG.Done()
		}(marshaller)
	}
}

// batchWriteRetry : retry logic wrapping the batchWriter
func (tb *table) batchWriteRetry(bi dynamodb.BatchWriteItemInput) {

	operation := func() error { //trying to get into the correct type for backoff
		return batchWriter(tb, bi)
	}

	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	if err != nil { // still errors after backoff then stop
		fmt.Println("writer failed, goroutine terminated ", err)
		return
	}
	return
}

// batchWriter : write batches of items to Dynamo Table
func batchWriter(tb *table, bi dynamodb.BatchWriteItemInput) error {
	req := tb.Svc.BatchWriteItemRequest(&bi)
	result, err := req.Send(context.Background())
	if len(result.UnprocessedItems) != 0 {
		err = errors.New("Unprocessed Items") // as well as dynamodb errors, force Retry
	}
	return err
}

func main() {

	itemCH := make(chan map[string]dynamodb.AttributeValue)
	paginateTablePool(&chMetaTable, itemCH)

}

// marshall a cHMetaItem struct into a []map[string][string]
// func marchallAttrs(st *cHMetaItem) []map[string]string {
// 	start := time.Now()
// 	xItems := []map[string]string{}
//
// 	for _, acct := range st.accounts {
// 		tempMap := map[string]string{}
//
// 		tempMap["name"] = st.name
// 		tempMap["number"] = st.number
// 		tempMap["contentFile"] = acct.contentFile
// 		tempMap["description"] = acct.description
// 		tempMap["docUrl"] = acct.docURL
// 		tempMap["madeUpDate"] = acct.madeUpDate
//
// 		xItems = append(xItems, tempMap)
//
// 	}
// 	t1 := time.Now()
// 	fmt.Println("marshal duration:", t1.Sub(start))
// 	return xItems
// }

// // PutAttrsItem : create a new item with scalar attributes
// func (tb *table) PutAttrsItem(a map[string]string) (*dynamodb.PutItemOutput, error) {
// 	start := time.Now()
//
// 	// errorReturnValue := dynamodb.PutItemOutput{}
//
// 	//initialise the map of attrVals for input
// 	attrValMap := make(map[string]dynamodb.AttributeValue)
//
// 	// add the data Attributes to input data
// 	for k, v := range a { //range over the map to get each attribute
// 		val, err := dynamodbattribute.Marshal(v)
// 		if err != nil {
// 			fmt.Println("error marshalling")
// 			// return &errorReturnValue, err
// 		}
//
// 		switch k {
// 		case "number":
// 			{
// 				attrValMap[tb.PrimaryKey] = *val
// 			}
// 		case "madeUpDate":
// 			{
// 				attrValMap[tb.SearchKey] = *val
// 			}
// 		default:
// 			{
// 				attrValMap[k] = *val
// 			}
// 		}
// 	}
//
// 	fmt.Println("input Attrs:", attrValMap)
//
// 	ItemInput := &dynamodb.PutItemInput{
// 		TableName:              aws.String(tb.TableName),
// 		Item:                   attrValMap,
// 		ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityTotal,
// 	}
//
// 	req := tb.Svc.PutItemRequest(ItemInput)
// 	result, err := req.Send(context.Background())
// 	if err != nil {
// 		// dynamodbErrorhandler(err)
// 		fmt.Println("PutItem response error:", err)
// 	}
// 	t1 := time.Now()
// 	fmt.Println("PutItem duration:", t1.Sub(start))
//
// 	return result, err
// }

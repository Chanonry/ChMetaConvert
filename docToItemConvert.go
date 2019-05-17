// Package ChMetaConvert : converts the CHMetaTest table document format to a new List Item format table
// utility progam
package ChMetaConvert

import (
	// "context"
	// "fmt"

	// import command syntax - go get github.com/Chanonry/dbhelpers
	"context"
	"fmt"
	"time"

	DbHelpers "github.com/Chanonry/dbhelpers"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/davecgh/go-spew/spew"
)

const version = "1.0.0" //synchronous

// performance:
// total 6.255s for 1 company 23 document records
// UnMarshal 213 ms
// Marshal 22 µs
// PutItem 200ms x 23
// my internet

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

// extract one item from target table and unmarshal into cHMetaItem struct
func unMarshallItem(tab *DbHelpers.DynamoTable) cHMetaItem {

	start := time.Now()

	res, err := tab.GetItem("00363849", "")
	if err != nil {
		fmt.Println("GetItem error:", err)
	}

	var coMeta = cHMetaItem{} //initialise the target struct for Unmarshaling
	//range across the top 3 fields - Name, Number and Accounts
	for k, v := range res.Item { // just the Item data no metadata or headers
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
	}
	t1 := time.Now()
	fmt.Println("UnMarshal duration:", t1.Sub(start))
	return coMeta
}

// marshall a cHMetaItem struct into a []map[string][string]
func marchallAttrs(st *cHMetaItem) []map[string]string {
	start := time.Now()
	xItems := []map[string]string{}

	for _, acct := range st.accounts {
		tempMap := map[string]string{}

		tempMap["name"] = st.name
		tempMap["number"] = st.number
		tempMap["contentFile"] = acct.contentFile
		tempMap["description"] = acct.description
		tempMap["docUrl"] = acct.docURL
		tempMap["madeUpDate"] = acct.madeUpDate

		xItems = append(xItems, tempMap)

	}
	t1 := time.Now()
	fmt.Println("marshal duration:", t1.Sub(start))
	return xItems
}

// PutAttrsItem : create a new item with scalar attributes
func (tb *table) PutAttrsItem(a map[string]string) (*dynamodb.PutItemOutput, error) {
	start := time.Now()

	// errorReturnValue := dynamodb.PutItemOutput{}

	//initialise the map of attrVals for input
	attrValMap := make(map[string]dynamodb.AttributeValue)

	// add the data Attributes to input data
	for k, v := range a { //range over the map to get each attribute
		val, err := dynamodbattribute.Marshal(v)
		if err != nil {
			fmt.Println("error marshalling")
			// return &errorReturnValue, err
		}

		switch k {
		case "number":
			{
				attrValMap[tb.PrimaryKey] = *val
			}
		case "madeUpDate":
			{
				attrValMap[tb.SearchKey] = *val
			}
		default:
			{
				attrValMap[k] = *val
			}
		}
	}

	fmt.Println("input Attrs:", attrValMap)

	ItemInput := &dynamodb.PutItemInput{
		TableName:              aws.String(tb.TableName),
		Item:                   attrValMap,
		ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityTotal,
	}

	req := tb.Svc.PutItemRequest(ItemInput)
	result, err := req.Send(context.Background())
	if err != nil {
		// dynamodbErrorhandler(err)
		fmt.Println("PutItem response error:", err)
	}
	t1 := time.Now()
	fmt.Println("PutItem duration:", t1.Sub(start))

	return result, err
}

func main() {
	start := time.Now()
	coStruct := unMarshallItem(&chMetaTable)
	// spew.Dump(coStruct)

	xI := marchallAttrs(&coStruct)
	spew.Dump(xI)

	for _, item := range xI {
		res, err := acctsMetaData.PutAttrsItem(item)
		if err != nil {
			fmt.Println("Put Item error")
		}
		fmt.Println(res)
	}
	t1 := time.Now()
	fmt.Println("total duration:", t1.Sub(start))

}

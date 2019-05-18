package ChMetaConvert

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// not proper isolated testing just functional tests for app

func TestPaginateTable(t *testing.T) {

	start := time.Now()
	testCH := make(chan map[string]dynamodb.AttributeValue)
	testMarshallCH := make(chan map[string]dynamodb.AttributeValue)
	total := 0

	paginateTablePool(&chMetaTable, testCH) //launches goroutines
	acctsMetaData.unMarshallPool(testCH, testMarshallCH)

	go func() {
		for _ = range testMarshallCH {
			total++
		}
	}()

	scannerWG.Wait()
	fmt.Println("closing testCH")
	close(testCH)
	marshallerWG.Wait()
	fmt.Println("closing testMarshallCH")
	close(testMarshallCH)

	t1 := time.Now()
	fmt.Println("Duration:", t1.Sub(start).String())
	fmt.Println("Total:", total)
	// if testPages == 1 && total != 353 {
	// 	t.Error("Incorrect total items: Expected 353 Got ", total)
	// } else {
	// 	if testPages != 1 {
	// 		t.Error("Testpages != 1, invalid results of test. Testpages: ", testPages)
	// 	}
	// }
}

func TestMain(t *testing.T) {
	main()
}

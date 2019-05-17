package ChMetaConvert

import "testing"

func TestUnMarshallItem(t *testing.T) {
	// testing against existing db service

	// problem here is that if this record gets updated then the test will fail due to too many accounts records
	res := unMarshallItem(&chMetaTable)
	if res.name != "GRAYDON UK LIMITED" {
		t.Error("UnMarshal error - wrong name:", res.name)
	}
	if res.number != "00363849" {
		t.Error("UnMarshal error - wrong number:", res.number)
	}
	if len(res.accounts) != 23 {
		t.Error("UnMarshal error - wrong number of accounts strucrs:", len(res.accounts))
	}

}

func TestMain(t *testing.T) {
	main()
}

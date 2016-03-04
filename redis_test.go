package nosql_test

import (
	"testing"

	"github.com/polaris1119/nosql"
)

func TestHSCAN(t *testing.T) {
	redisClient := nosql.NewRedisClient()
	defer redisClient.Close()

	_, _, err := redisClient.HSCAN("store:691goods", 0)
	if err != nil {
		t.Fatal("HSCAN error:", err)
	}
}

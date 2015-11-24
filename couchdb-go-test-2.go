package main

import (
	"fmt"
	"log"

	"github.com/couchbaselabs/go-couchbase"
)

// Go representation of a JSON structure with a couple of fields
// name and id
//
// NOTE: Store data into a a single couchebase node installation works properly,
//       but, can not achive to save data into a docker couchbase container.
type User struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

func main() {

	connection, err := couchbase.Connect("http://192.168.99.100:32778")
	if err != nil {
		log.Fatalf("Failed to connect to couchbase (%s)\n", err)
	}

	pool, err := connection.GetPool("default")
	if err != nil {
		log.Fatalf("Failed to get pool from couchbase (%s)\n", err)
	}

	bucket, err := pool.GetBucket("default2")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	user := User{"Frank", "1"}

	added, err := bucket.Add(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	if !added {
		log.Fatalf("A Document with the same id of (%s) already exists.\n", user.Id)
	}

	// Go automatically parse the JSON into the User struct
	user = User{}

	err = bucket.Get("1", &user)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}
	fmt.Printf("Got back a user with a name of (%s) and id (%s)\n", user.Name, user.Id)

}

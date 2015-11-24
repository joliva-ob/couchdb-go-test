package main

import (
	"fmt"

	"github.com/couchbaselabs/go-couchbase"
	//"github.com/davecgh/go-spew/spew"
	"encoding/json"

	"github.com/kr/pretty"
	//"reflect"
	"errors"
	"runtime"
	"strings"
)

var FILE_PATH string
var CB *couchbase.Bucket

// initialize file path
func init() {
	_, file, _, _ := runtime.Caller(1)
	FILE_PATH = file[:4+strings.Index(file, "/tmp/")]
	err := errors.New("no error")
	CB, err = couchbase.GetBucket("http://127.0.0.1:8091/", "default", "default")
	fmt.Println("Couchbase connection initialized.")
	Panic(err, "Error connection, getting pool or bucket:  %v")
}

// print warning message
func Check(err error, msg string, args ...interface{}) error {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		str := fmt.Sprintf("%s:%d: ", file[len(FILE_PATH):], line)
		fmt.Errorf(str+msg, args...)
		res := pretty.Formatter(err)
		fmt.Errorf("%# v\n", res)
	}
	return err
}

// print error message and exit program
func Panic(err error, msg string, args ...interface{}) {
	if Check(err, msg, args...) != nil {
		panic(err)
	}
}

// describe a variable
func Explain(args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	//res, _ := json.MarshalIndent(variable, "   ", "  ")
	for _, arg := range args {
		res := pretty.Formatter(arg)
		fmt.Printf("%s:%d: %# v\n", file[len(FILE_PATH):], line, res)
	}
	//spew.Dump(variable)
}

func main() {

	var err error

	// save values (upsert)
	fmt.Println("Setting someKey...")
	err = CB.Set("someKey", 0, []string{"an", "example", "list"})
	Check(err, "failed to set somekey")

	fmt.Println("Setting primaryKey...")
	err = CB.Set("primaryKey", 0, 1)
	Check(err, "failed to set primaryKey")

	// fetch one value
	fmt.Println("Getting someKey value...")
	var rv interface{}
	err = CB.Get("someKey", &rv)
	Check(err, "failed to get someKey")
	Explain(rv)

	// fetch with CheckAndSet id
	fmt.Println("Getting by primaryKey...")
	cas := uint64(0)
	err = CB.Gets("primaryKey", &rv, &cas)
	Check(err, "failed to get primaryKey")
	Explain(cas, rv)

	// fetch multivalue
	rows, err := CB.GetBulk([]string{"someKey", "primaryKey", "nothingKey"})
	Check(err, "failed to get someKey or primaryKey or nothingKey")
	Explain(rows)

	jsonStr := rows["someKey"].Body
	Explain(string(jsonStr))

	stringList := []string{}
	err = json.Unmarshal(jsonStr, &stringList)
	Check(err, "failed to convert back to json")
	Explain(stringList)

	// increment value, returns new value
	nv, err := CB.Incr("primaryKey", 1, 1, 0)
	Check(err, "failed to increment primaryKey")
	Explain(nv)

	// increment value, defaults to 1 if not exists
	nv, err = CB.Incr("key3", 1, 1, 60)
	Check(err, "failed to increment primaryKey")
	Explain(nv)

}

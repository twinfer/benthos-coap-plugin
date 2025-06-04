package main

import (
	"fmt"
	"reflect"

	"github.com/plgd-dev/go-coap/v3/message"
)

func main() {
	// Check how to create and manipulate Options
	var opts message.Options
	fmt.Printf("Options type: %T\n", opts)

	// Check methods on Options
	val := reflect.ValueOf(&opts)
	typ := val.Type()

	fmt.Printf("Methods on Options:\n")
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		fmt.Printf("  %s: %v\n", method.Name, method.Type)
	}

	// Check how to create Option
	var opt message.Option
	fmt.Printf("\nOption struct: %+v\n", opt)

	// Check MediaType
	fmt.Printf("AppJSON MediaType: %v\n", message.AppJSON)
}

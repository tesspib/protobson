# protobson

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)](https://pkg.go.dev/mod/github.com/ThePib/protobson)
[![Go Report Card](https://goreportcard.com/badge/github.com/ThePib/protobson)](https://goreportcard.com/report/github.com/ThePib/protobson)

## Description

`protobson` is a Go library consisting of a BSON codec for Protobuf messages that can be used with [`mongo-go-driver`](https://github.com/mongodb/mongo-go-driver).

This library uses the second major version of the [Go Protobuf API](https://pkg.go.dev/mod/google.golang.org/protobuf).

## Overview

- [Usage](#usage)
- [Credits](#Credits)

## Usage

Below is a snippet making use of this codec by registering it with the MongoDB Go library:

```go
package main

import (
    "reflect"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo/options"
    "google.golang.org/protobuf/proto"

    "github.com/ThePib/protobson"
)

func main() {
    regBuilder := bson.NewRegistryBuilder()
    codec := protobson.NewCodec()

    msgType := reflect.TypeOf((*proto.Message)(nil)).Elem()
    registry := regBuilder.RegisterHookDecoder(msgType, codec).RegisterHookEncoder(msgType, codec).Build()

    opts := options.Client().SetRegistry(registry)
    // opts.ApplyURI("mongodb://localhost:27017")
    // ...
}
```

Note the use of `RegisterHookDecoder` and `RegisterHookEncoder` methods. Those ensure that given codec will be used to encode and decode values which type implement the interface. Since every Protobuf message implements the `proto.Message` interface, the codec will work with any message value.

## Credits

This library is originally based on [`protomongo`](https://github.com/dataform-co/dataform/blob/master/protomongo), part of the MIT-licensed [`dataform`](https://github.com/dataform-co/dataform) project by Tada Science, Inc.

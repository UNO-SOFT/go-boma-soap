// Copyright 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package main

import (
	"flag"
	"log"

	"github.com/UNO-SOFT/go-boma-soap/enums"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

func Main() error {
	flagPackage := flag.String("package", "enums", "package name")
	flagName := flag.String("name", "Enums", "map name")
	flagOut := flag.String("o", "-", "output file name")
	flag.Parse()

	m, err := enums.ParseFiles(flag.Args()...)
	if err != nil {
		return err
	}
	return enums.WriteGeneratedFile(*flagOut, *flagPackage, *flagName, m)
}

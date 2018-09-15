package main

import (
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/relops/cqlc/generator"
)

var opts generator.Options
var parser = flags.NewParser(&opts, flags.Default)

var Version = "0.10.5"

func init() {
	opts.Version = printVersionAndExit
}

func main() {

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	if err := generator.Generate(&opts, Version); err != nil {
		if err == generator.ErrInvalidOptions {
			parser.WriteHelp(os.Stderr)
			os.Exit(1)
		} else {
			log.Fatalln(err)
		}
	}
}

func printVersionAndExit() {
	os.Stdout.Write([]byte(Version + "\n"))
	os.Exit(0)
}

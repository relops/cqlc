// +build ignore

package main

import (
	"bytes"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/relops/cqlc/meta"
	"io/ioutil"
	"os"
	"text/template"
)

var logConfig = `
<seelog type="sync">
	<outputs formatid="main">
		<console/>
	</outputs>
	<formats>
		<format id="main" format="%Date(2006-02-01 03:04:05.000) - %Msg%n"/>
	</formats>
</seelog>`

func init() {
	logger, err := log.LoggerFromConfigAsString(logConfig)

	if err != nil {
		fmt.Printf("Could not load seelog configuration: %s\n", err)
		return
	}

	log.ReplaceLogger(logger)
}

func main() {
	params := make(map[string]interface{})
	params["basicTypes"] = meta.DataTypes

	t, err := template.New("validator_parser.tmpl").ParseFiles("tmpl/validator_parser.tmpl")
	if err != nil {
		log.Errorf("Could not open template: %s", err)
		return
	}

	var b bytes.Buffer
	t.Execute(&b, params)

	if err := ioutil.WriteFile("validator_parser.go", b.Bytes(), os.ModePerm); err != nil {
		log.Errorf("Could not write templated file: %s", err)
		return
	}

	log.Info("Regenerated data types")
}

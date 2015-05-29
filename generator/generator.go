package generator

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"go/format"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrInvalidOptions = errors.New("invalid options")
)

type Options struct {
	Instance string `short:"i" long:"instance" description:"The Cassandra instance to connect to"`
	Keyspace string `short:"k" long:"keyspace" description:"The keyspace that contains the target schema"`
	Package  string `short:"p" long:"package" description:"The name of the target package for the generated code"`
	Output   string `short:"o" long:"output" description:"The file to write the generated bindings to"`
	Version  func() `short:"V" long:"version" description:"Print cqlc version and exit"`
	Verbose  []bool `short:"v" long:"verbose" description:"Show verbose debug information"`
	Symbols  bool   `short:"s" long:"symbols" description:"Generate compile symbols for each column family"`
	Username string `short:"u" long:"username" description:"Username for authentication"`
	Password string `short:"w" long:"password" description:"Password for authentication"`
}

type Provenance struct {
	Keyspace      string
	Version       string
	Timestamp     time.Time
	NegotiatedCQL string
	ServerCQL     string
	ServerRelease string
	HostId        gocql.UUID
}

func Generate(opts *Options, version string) error {

	err := validateOptions(opts)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	if err = generateBinding(opts, version, &b); err != nil {
		return err
	}
	if b.Len() > 0 {
		if err := os.MkdirAll(filepath.Dir(opts.Output), 0755); err != nil {
			return err
		}
		if err := ioutil.WriteFile(opts.Output, b.Bytes(), 0644); err != nil {
			return err
		}
	}
	return nil
}

func validateOptions(opts *Options) error {
	if opts.Instance == "" || opts.Keyspace == "" || opts.Package == "" || opts.Output == "" {
		return ErrInvalidOptions
	}
	if (opts.Username == "" && opts.Password != "") || (opts.Username != "" && opts.Password == "") {
		return ErrInvalidOptions
	}
	return nil
}

func coalesceImports(md *gocql.KeyspaceMetadata) []string {

	set := make(map[string]bool)
	for _, path := range importPaths(md) {
		set[path] = true
	}

	set["github.com/relops/cqlc/cqlc"] = true
	set["github.com/gocql/gocql"] = true
	set["log"] = true

	paths := make([]string, 0)
	for path, _ := range set {
		paths = append(paths, path)
	}

	return paths
}

func generateBinding(opts *Options, version string, w io.Writer) error {

	cluster := gocql.NewCluster(opts.Instance)

	if opts.Username != "" && opts.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: opts.Username,
			Password: opts.Password,
		}
	}

	s, err := cluster.CreateSession()

	if err != nil {
		return fmt.Errorf("Connect error", err)
	}

	defer s.Close()

	var release, cqlVersion string
	var hostId gocql.UUID
	err = s.Query(`SELECT release_version, cql_version, host_id
		           FROM system.local`).Scan(&release, &cqlVersion, &hostId)
	if err != nil {
		return fmt.Errorf("System metadata error", err)
	}

	md, err := s.KeyspaceMetadata(opts.Keyspace)

	if err != nil {
		return err
	}

	provenance := Provenance{
		Keyspace:      opts.Keyspace,
		Version:       version,
		Timestamp:     time.Now(),
		HostId:        hostId,
		NegotiatedCQL: cluster.CQLVersion,
		ServerCQL:     cqlVersion,
		ServerRelease: release,
	}

	meta := make(map[string]interface{})
	meta["Provenance"] = provenance
	meta["Options"] = opts
	meta["Imports"] = coalesceImports(md)
	meta["Tables"] = md.Tables

	var b bytes.Buffer
	if err := bindingTemplate.Execute(&b, meta); err != nil {
		return err
	}

	bfmt, err := format.Source(b.Bytes())
	if err != nil {
		return err
	}

	if _, err := w.Write(bfmt); err != nil {
		return err
	}

	return nil
}

func importPaths(md *gocql.KeyspaceMetadata) (imports []string) {
	// Ideally need to use a set
	paths := make(map[string]bool)

	f := func(t gocql.TypeInfo) {
		literal := literalTypes[t.Type()]
		if strings.Contains(literal, ".") {
			paths[literal] = true
		}
	}

	for _, table := range md.Tables {
		for _, col := range table.Columns {
			t := col.Type
			switch t.Type() {
			case gocql.TypeList, gocql.TypeSet:
				// TODO should probably not swallow this
				ct, _ := t.(gocql.CollectionType)
				f(ct.Elem)
			case gocql.TypeMap:
				// TODO should probably not swallow this
				ct, _ := t.(gocql.CollectionType)
				f(ct.Key)
				f(ct.Elem)
			default:
				nt, _ := t.(gocql.NativeType)
				f(nt)
			}
		}
	}

	for path, _ := range paths {
		customPath, present := customImportPaths[path]
		if present {
			imports = append(imports, customPath)
		} else {
			parts := strings.Split(path, ".")
			imports = append(imports, parts[0])
		}
	}

	return imports
}

package generator

import (
    "bytes"
    "errors"
    "go/format"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
)

var (
    ErrInvalidOptions = errors.New("invalid options")
)

type Options struct {
    Instance string `short:"i" long:"instance" description:"The Cassandra instance to connect to"`
    Keyspace string `short:"k" long:"keyspace" description:"The keyspace to that contains the target schema"`
    Package  string `short:"p" long:"package" description:"The name of the target package for the generated code"`
    Output   string `short:"o" long:"output" description:"The file to write the generated bindings to"`
    Version  func() `short:"V" long:"version" description:"Print cqlc version and exit"`
}

func Generate(opts *Options) error {

    err := validateOptions(opts)
    if err != nil {
        return err
    }

    var b bytes.Buffer
    if err = generateBinding(opts, &b); err != nil {
        return err
    }
    if b.Len() > 0 {
        if err := os.MkdirAll(filepath.Dir(opts.Output), os.ModePerm); err != nil {
            return err
        }
        if err := ioutil.WriteFile(opts.Output, b.Bytes(), os.ModePerm); err != nil {
            return err
        }
    }
    return nil
}

func validateOptions(opts *Options) error {
    if opts.Instance == "" || opts.Keyspace == "" || opts.Package == "" || opts.Output == "" {
        return ErrInvalidOptions
    }
    return nil
}

func generateBinding(opts *Options, w io.Writer) error {

    cf, err := ColumnFamilies(opts.Instance, opts.Keyspace)

    if err != nil {
        return err
    }

    meta := make(map[string]interface{})
    meta["Options"] = opts
    meta["Imports"] = importPaths(cf)
    meta["ColumnFamilies"] = cf

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
func importPaths(families []ColumnFamily) (imports []string) {
    // Ideally need to use a set
    paths := make(map[string]bool)

    for _, cf := range families {
        for _, col := range cf.Columns {
            literal := literalTypes[col.DataType]
            if strings.Contains(literal, ".") {
                paths[literal] = true
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

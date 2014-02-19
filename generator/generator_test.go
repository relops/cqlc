package generator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var opts *Options

func init() {
	opts = &Options{
		Package:  "main",
		Instance: "127.0.0.1",
		Keyspace: "cqlc",
	}
}

func TestBasicGenerator(t *testing.T) {

	out, err := runFixture("basic", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestCounterGenerator(t *testing.T) {

	out, err := runFixture("counter", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestDeleteGenerator(t *testing.T) {
	out, err := runFixture("delete", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestComparisonGenerator(t *testing.T) {

	out, err := runFixture("comparison", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestBatchGenerator(t *testing.T) {

	out, err := runFixture("batch", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestSensorGenerator(t *testing.T) {

	out, err := runFixture("sensor", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestBindGenerator(t *testing.T) {

	out, err := runFixture("bind", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestIntoGenerator(t *testing.T) {

	out, err := runFixture("into", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestBlobGenerator(t *testing.T) {

	out, err := runFixture("blob", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

func TestCompositionGenerator(t *testing.T) {

	out, err := runFixture("composition", opts)

	assert.NoError(t, err)
	assert.Equal(t, out, "PASSED")
}

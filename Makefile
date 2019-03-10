VERSION = 0.13.0
LDFLAGS = -X main.Version=$(VERSION)
GO = CGO_ENABLED=0 go
GO_LINUX = GOOS=linux GOARCH=amd64 $(GO)
GO_MAC = GOOS=darwin GOARCH=amd64 $(GO)
GO_WINDOWS = GOOS=windows GOARCH=amd64 $(GO)

.PHONY: gen fmt build install test

fmt:
	gofmt -d -l -w cqlc generator e2e

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o build/cqlc .

build-all: build-linux build-mac build-windows

build-linux:
	$(GO_LINUX) build -ldflags "$(LDFLAGS)" -o build/cqlc-linux .

build-mac:
	$(GO_MAC) build -ldflags "$(LDFLAGS)" -o build/cqlc-mac .

build-windows:
	$(GO_WINDOWS) build -ldflags "$(LDFLAGS)" -o build/cqlc-windows .

install:
	go install -ldflags "$(LDFLAGS)" .

# sync the version defined in runtime with Makefile
update-ver:
# NOTE: mac's default sed is not GNU sed https://stackoverflow.com/questions/4247068/sed-command-with-i-option-failing-on-mac-but-works-on-linux
	sed -i .bak -E 's/const Version = "(.*)"/const Version = "$(VERSION)"/g' cqlc/ver.go

release: update-ver build-all
	cd build; rm -f *.zip
	cd build; zip cqlc-$(VERSION)-linux.zip cqlc-linux
	cd build; zip cqlc-$(VERSION)-mac.zip cqlc-mac
	cd build; zip cqlc-$(VERSION)-windows.zip cqlc-windows

# generate highly duplicated part in runtime
gen:
	cd cqlc; go run column_generator.go

test: test-unit

test-unit:
	go test -v ./cqlc

travis-test: install
	docker-compose -f e2e/docker-compose.yaml up -d c2
	./wait-on-c.sh
	docker ps
	sleep 5
	go test -v ./e2e

travis-tear:
	cd e2e && make down
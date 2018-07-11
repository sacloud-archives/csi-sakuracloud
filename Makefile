all: fmt test

fmt:
	go fmt ./...

test:
	go test -v ./...

.PHONY: all fmt test

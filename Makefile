# Common tasks. Tools: Go 1.26, controller-gen, docker, kind, kubectl.
CONTROLLER_GEN ?= controller-gen

.PHONY: build test vet generate images e2e

build:
	go build ./...

vet:
	go vet ./...
	test -z "$$(gofmt -l .)"

test:
	go test ./...

generate:
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd

images:
	docker build --target controller -t wsnap-controller:dev .
	docker build --target agent -t wsnap-agent:dev .

# Needs a kind cluster named wsnap; see docs/verification.md.
e2e:
	test/e2e/run.sh

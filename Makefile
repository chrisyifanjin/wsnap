# Common tasks. Tools: Go 1.26, controller-gen, docker, kind, kubectl.
CONTROLLER_GEN ?= controller-gen

.PHONY: build test vet generate images e2e install-manifest

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

VERSION ?= dev
REGISTRY ?= ghcr.io/chrisyifanjin
CONTROLLER_IMAGE ?= $(REGISTRY)/wsnap-controller:$(VERSION)
AGENT_IMAGE ?= $(REGISTRY)/wsnap-agent:$(VERSION)

images:
	docker build --target controller --build-arg VERSION=$(VERSION) -t wsnap-controller:dev .
	docker build --target agent --build-arg VERSION=$(VERSION) -t wsnap-agent:dev .

# install.yaml: CRDs, RBAC and the controller with explicit image references.
# Release builds pass immutable digests; the default version tags are useful
# for local rendering. The agent image is a controller argument, so rewrite it
# as well and fail if either source reference remains.
install-manifest:
	kubectl kustomize config/default | \
	  sed -e 's#image: wsnap-controller:dev#image: $(CONTROLLER_IMAGE)#' \
	      -e 's#--agent-image=wsnap-agent:dev#--agent-image=$(AGENT_IMAGE)#' > install.yaml
	test "$$(grep -F -c -- 'image: $(CONTROLLER_IMAGE)' install.yaml)" -eq 1
	test "$$(grep -F -c -- '--agent-image=$(AGENT_IMAGE)' install.yaml)" -eq 1
	! grep -E 'wsnap-(controller|agent):dev' install.yaml

# Needs a kind cluster named wsnap; see docs/verification.md.
e2e:
	test/e2e/run.sh

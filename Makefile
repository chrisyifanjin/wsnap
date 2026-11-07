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

images:
	docker build --target controller --build-arg VERSION=$(VERSION) -t wsnap-controller:dev .
	docker build --target agent --build-arg VERSION=$(VERSION) -t wsnap-agent:dev .

# install.yaml: CRDs, RBAC and the controller with released image tags.
# The agent image is a controller argument, so it is rewritten too; the
# grep fails the build if either rewrite did not happen.
install-manifest:
	kubectl kustomize config/default | \
	  sed -e 's#image: wsnap-controller:dev#image: $(REGISTRY)/wsnap-controller:$(VERSION)#' \
	      -e 's#--agent-image=wsnap-agent:dev#--agent-image=$(REGISTRY)/wsnap-agent:$(VERSION)#' > install.yaml
	test "$$(grep -c '$(REGISTRY)/wsnap-.*:$(VERSION)' install.yaml)" -eq 2

# Needs a kind cluster named wsnap; see docs/verification.md.
e2e:
	test/e2e/run.sh

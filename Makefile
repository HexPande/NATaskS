ifeq (, $(shell which golangci-lint))
$(warning "could not find golangci-lint in $(PATH), run: curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh")
endif

lint:
	$(info ******************** running lint tools ********************)
	golangci-lint run

test: install_deps
	$(info ******************** running tests ********************)
	go test -v ./...

docker-test:
	$(info ******************** running tests in docker ********************)
	@bash -c 'docker compose up --build --abort-on-container-exit --exit-code-from test; status=$$?; docker compose down -v; exit $$status'

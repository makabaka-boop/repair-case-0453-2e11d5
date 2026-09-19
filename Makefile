.PHONY: build test test-short verify up down acceptance tidy fmt vet

build:
	go build ./...

# Tests require a real PostgreSQL. Set TEST_DATABASE_URL to point at it.
# Default matches the compose database service.
test:
	go test -race -count=1 ./...

test-short:
	go test -short ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

up:
	docker compose up -d --build db api1 api2

down:
	docker compose down -v

# One-shot acceptance (protocol matrix + cross-instance race).
verify:
	docker compose run --rm verify

# Full acceptance including a restart of both API instances.
acceptance:
	./scripts/acceptance.sh

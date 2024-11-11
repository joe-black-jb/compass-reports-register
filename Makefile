.PHONY: xbrl local lint fmt

lint:
	go vet ./...

fmt:
	go fmt ./...

xbrl:
	ENV=local go run .

local:
	ENV=local go run ./local/local.go

deploy:
	sh ./scripts/deploy.sh

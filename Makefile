.PHONY: build run clean

build:
	@echo "Building nats-lab..."
	@go build -o nats-lab ./cmd/server/
	@echo "Done → ./nats-lab"

run: build
	@./nats-lab

clean:
	@rm -f nats-lab

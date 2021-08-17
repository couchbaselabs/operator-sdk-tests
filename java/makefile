.DEFAULT_GOAL := container

all: local container
container:
	docker build -t javasdk .
local:
	mvn clean compile dependency:copy-dependencies assembly:assembly
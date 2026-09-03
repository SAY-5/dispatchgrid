MVN ?= mvn
TAG ?= dev
COMPOSE := docker compose -f deploy/docker-compose.yml
MODULES := rider-request-service driver-location-service matching-service loadgen

.PHONY: build test lint fmt images demo demo-down k8s-e2e clean

build:
	$(MVN) -B -DskipTests package

test:
	$(MVN) -B verify

lint:
	$(MVN) -B spotless:check

fmt:
	$(MVN) -B spotless:apply

images:
	@for m in $(MODULES); do \
	  echo "building dispatchgrid/$$m:$(TAG)"; \
	  docker build --build-arg MODULE=$$m -t dispatchgrid/$$m:$(TAG) . || exit 1; \
	done

demo:
	$(COMPOSE) up -d --build --wait redpanda mysql-shard-0 mysql-shard-1 redis
	$(COMPOSE) up -d --build rider-request-service driver-location-service matching-service
	$(COMPOSE) --profile loadgen run --rm --build loadgen

demo-down:
	$(COMPOSE) --profile loadgen down -v --remove-orphans

k8s-e2e:
	./scripts/k8s-e2e.sh

clean:
	$(MVN) -B -q clean

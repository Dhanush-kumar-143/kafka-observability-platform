.PHONY: up down logs ps lag-demo lag-reset test lint build scan clean

up:            ## Build and start the full stack
	docker compose up -d --build

down:          ## Stop the stack (keep data)
	docker compose down

clean:         ## Stop the stack and delete all volumes
	docker compose down -v

logs:          ## Follow producer and consumer logs
	docker compose logs -f producer consumer

ps:
	docker compose ps

lag-demo:      ## Slow the consumer down (3s per message) so lag builds and alerts fire
	CONSUMER_DELAY_MS=3000 docker compose up -d consumer

lag-reset:     ## Restore the normal consumer speed
	CONSUMER_DELAY_MS=0 docker compose up -d consumer

test:          ## Run unit tests
	pytest -q

lint:          ## Run flake8
	flake8 app tests

build:         ## Build both images
	docker build -t kafka-obs-app:local .
	docker build -t kafka-obs-kafka:local ./kafka

scan: build    ## Trivy scan (needs trivy installed)
	trivy image --severity HIGH,CRITICAL --ignore-unfixed kafka-obs-app:local

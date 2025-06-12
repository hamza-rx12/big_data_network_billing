.PHONY: build-java build-docker run clean

# Build all Java projects
build-java:
	@echo "Building producer..."
	cd producer && mvn clean package
	@echo "Building consumer..."
	cd consumer && mvn clean package
	@echo "Building database projects..."
	@echo "Building springboot_api..."
	cd database/springboot_api && mvn clean package -DskipTests

# Build Docker images
build-docker: build-java
	@echo "Building Docker images..."
	docker-compose build

# Run the application
run: build-docker
	@echo "Starting the application..."
	@trap 'make clean' INT TERM; \
	docker-compose up

# Run batch processing
batch-process:
	@echo "Starting batch processing..."
	cd batch_processor && mvn clean package
	cd batch_processor && docker build -t test .
	docker run --net kafka_my_net --name batchProcess test
	cd batch_processor && mvn clean
	docker rm batchProcess

# Generate invoices
generate-invoices:
	@echo "Generating invoices..."
	.venv/bin/python3 invoices/invoices.py

# Start dashboard
start-dashboard:
	@echo "Starting dashboard..."
	.venv/bin/streamlit run dashboard/app.py





# Clean up
clean:
	@echo "Cleaning up..."
	cd producer && mvn clean
	cd consumer && mvn clean
	cd database/springboot_api && mvn clean
	cd batch_processor && clean
	docker-compose down -v

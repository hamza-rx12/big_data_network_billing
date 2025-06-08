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


# Clean up
clean:
	@echo "Cleaning up..."
	cd producer && mvn clean
	cd consumer && mvn clean
	cd database/springboot_api && mvn clean
	docker-compose down -v

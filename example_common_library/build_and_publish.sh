#!/bin/bash

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Run `poetry build`
echo "Running poetry build..."
poetry build || handle_error "Failed to build the package."

# Run `poetry publish`
echo "Publishing package to repository ..."
poetry publish -r repo || handle_error "Failed to publish the package to repository.  Are you connected to the vpn?"

echo "Build and publish process completed successfully."
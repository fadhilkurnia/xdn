#!/bin/bash

#######################################################################
# This script launches 8 instances of the bookcatalog service (sv1-sv8)
# using the fadhilkurnia/xdn-bookcatalog Docker image. Each service is:
# - Launched with linearizability consistency
# - Uses deterministic execution
# - Stores state in /app/data/
# - Health-checked via HTTP endpoint
#
# Image used: fadhilkurnia/xdn-bookcatalog
# To load test those services, 
#  use k6_xdn_multi_bookcatalog_load_breakpoint.js script
#######################################################################

# Configuration
XDN_CONTROL_PLANE=10.10.1.4    # Control plane node IP
XDN_FIRST_NODE=10.10.1.1       # First node IP for health checks
MAX_RETRIES=30                 # Maximum health check attempts
SLEEP_BETWEEN_RETRIES=10       # Seconds between health check attempts
API_PORT=2300                  # Port for the API endpoint

# Function to launch a service
launch_service() {
    local svc_name=$1
    echo "Launching service ${svc_name}..."
    XDN_CONTROL_PLANE=$XDN_CONTROL_PLANE xdn launch ${svc_name} \
        --consistency=linearizability \
        --deterministic=true \
        --image=fadhilkurnia/xdn-bookcatalog \
        --state=/app/data/
}

# Function to check if a service is healthy
check_service_health() {
    local svc_name=$1
    local retry_count=0
    local is_healthy=false

    echo "Checking health of service ${svc_name}..."
    while [ $retry_count -lt $MAX_RETRIES ]; do
        if curl -s -f -H "XDN: ${svc_name}" http://${XDN_FIRST_NODE}:${API_PORT}/api/books > /dev/null 2>&1; then
            echo "Service ${svc_name} is healthy!"
            is_healthy=true
            break
        else
            echo "Service ${svc_name} not ready yet (attempt $((retry_count + 1))/${MAX_RETRIES})"
            sleep $SLEEP_BETWEEN_RETRIES
            retry_count=$((retry_count + 1))
        fi
    done

    if [ "$is_healthy" = false ]; then
        echo "ERROR: Service ${svc_name} failed to become healthy after ${MAX_RETRIES} attempts"
        return 1
    fi
    return 0
}

# Main script
echo "Starting to launch services..."

# Launch all services
for i in {1..8}; do
    service_name="sv${i}"
    launch_service $service_name
    sleep 5  # Brief pause between launches to prevent overwhelming the system
done

echo "All services launched, starting health checks..."

# Check health of all services
failed_services=()
for i in {1..8}; do
    service_name="sv${i}"
    if ! check_service_health $service_name; then
        failed_services+=($service_name)
    fi
done

# Final status report
if [ ${#failed_services[@]} -eq 0 ]; then
    echo "SUCCESS: All services are up and healthy!"
    exit 0
else
    echo "ERROR: The following services failed to start: ${failed_services[*]}"
    exit 1
fi
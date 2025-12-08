#!/bin/bash

# Simple script to generate RPC authentication secret for Spark

# Generate a random 64-byte hex string (256 bits) for the secret
SPARK_RPC_AUTHENTICATION_SECRET=$(openssl rand -hex 32)

# Print the generated secret
echo "Generated RPC Authentication Secret:"
echo "$SPARK_RPC_AUTHENTICATION_SECRET"

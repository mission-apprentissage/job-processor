#!/bin/bash
set -euo pipefail

# Start mongod process in the background
mongod --replSet "rs0" --bind_ip_all --port 27017 --keyFile /tmp/mongo_keyfile &

# Capture the PID of the mongod background process
MONGOD_PID=$!

# Wait for mongod to be up and running before attempting to connect
echo "Waiting for MongoDB to be ready..."
until mongosh "mongodb://__system:password@localhost:27017/?authSource=local&directConnection=true" --eval "print(\"waited for connection\")" &>/dev/null; do
    sleep 1
done
echo "MongoDB is ready."

# Init the replica set if it is not already initialized
mongosh "mongodb://__system:password@localhost:27017/?authSource=local&directConnection=true" --file /tmp/init.js
echo "MongoDB is initialized."

# The `wait` command will wait for the background `mongod` process to finish.
# This ensures the shell script doesn't exit, and the container stays running.
wait $MONGOD_PID
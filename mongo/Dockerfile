FROM mongo:8

# Copy the custom entrypoint script into the container
COPY ./entrypoint.sh /usr/local/bin/

# Copy the initialization script into the container
COPY ./init.js /tmp/init.js

# Make the script executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Create the keyfile for replica set authentication
RUN echo 'password' > /tmp/mongo_keyfile
RUN chmod 400 /tmp/mongo_keyfile
RUN chown mongodb:mongodb /tmp/mongo_keyfile

# Define a healthcheck to monitor the MongoDB replica set status.
# Docker will check if the mongod instance is ready to accept connections.
HEALTHCHECK --interval=10s --timeout=5s --retries=10 \
  CMD mongosh "mongodb://__system:password@localhost:27017/?authSource=local&directConnection=true" --eval 'db.runCommand("ping").ok' --quiet

# Set the custom script as the entrypoint for the container
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
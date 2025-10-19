#!/bin/bash

# Wait for RabbitMQ to start
echo "Waiting for RabbitMQ to start..."
sleep 10

# Enable plugins
echo "Enabling RabbitMQ plugins..."
rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
rabbitmq-plugins enable rabbitmq_shovel
rabbitmq-plugins enable rabbitmq_shovel_management
rabbitmq-plugins enable rabbitmq_auth_backend_ldap

echo "RabbitMQ plugins enabled successfully!"

FROM rabbitmq:3.13.6-management

# Copy initialization script
COPY rabbitmq-init.sh /usr/local/bin/rabbitmq-init.sh
RUN chmod +x /usr/local/bin/rabbitmq-init.sh

# Add this to ensure we start fresh
ENV RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_management force_fine_statistics true"
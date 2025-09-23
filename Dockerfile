# Use the official Apache NiFi image as the base image
FROM apache/nifi:1.25.0

USER root
# Set environment variables
ENV NIFI_HOME=/opt/nifi/nifi-current

# Copy custom scripts into the container
COPY ./scripts ${NIFI_HOME}/custom-scripts/

# Set permissions for the copied files
RUN chmod -R 755 ${NIFI_HOME}/custom-scripts

# install python
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install -r ${NIFI_HOME}/custom-scripts/requirements.txt

# Create the 'nifi' user if it doesn't exist
RUN id -u nifi &>/dev/null || useradd -m -d /home/nifi -s /bin/bash nifi




USER nifi
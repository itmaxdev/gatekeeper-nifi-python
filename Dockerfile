# Use the official Apache NiFi image as the base image
FROM apache/nifi:1.25.0

USER root
# Set environment variables



# install python
RUN apt-get update && apt-get install -y python3 python3-pip
COPY ./scripts/requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt





USER nifi
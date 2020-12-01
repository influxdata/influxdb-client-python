FROM amazonlinux:2
RUN mkdir /install
WORKDIR /install
RUN yum install -y amazon-linux-extras
RUN amazon-linux-extras enable python3.8
RUN yum install -y python38 python38-devel python3-pip zip gcc
RUN python3.8 -m pip install --upgrade pip && \
    python3.8 -m pip install virtualenv
RUN python3.8 -m venv lambda
RUN source lambda/bin/activate
# Python dependencies to be included in output zip file:
RUN python3.8 -m pip install influxdb-client[ciso] -t /install/python
# Create zip file
RUN zip -r /install/python.zip python/
VOLUME ["/install"]
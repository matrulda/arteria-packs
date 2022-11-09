From centos:7

workdir /tmp
RUN yum install -y epel-release
RUN yum update -y
RUN yum install -y unzip python-pip python-virtualenv git gcc-c++ wget
COPY bcl2fastq2-v2.20.0.422-Linux-x86_64.rpm /tmp
RUN yum install -y /tmp/bcl2fastq2-v2.20.0.422-Linux-x86_64.rpm
RUN yum install -y python3

# setup arteria-runfolder service
WORKDIR /
RUN git clone https://github.com/arteria-project/arteria-bcl2fastq.git
RUN pip3 install -r ./arteria-bcl2fastq/requirements/dev ./arteria-bcl2fastq
COPY app.config ./arteria-bcl2fastq/config/
RUN mkdir /bcl2fastq-logs

EXPOSE 80
CMD bcl2fastq-ws --debug --port 80 --config ./arteria-bcl2fastq/config/


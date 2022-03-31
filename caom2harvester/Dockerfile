FROM cadc-java:latest

RUN yum install -y wcslib && yum clean all

ADD build/distributions/caom2harvester-2.5.6.tar /opt/

RUN touch /root/.dbrc && echo "sync-pg cadctest cadmin pw-cadmin org.postgresql.Driver jdbc:postgresql://localhost:7432/cadctest" > /root/.dbrc

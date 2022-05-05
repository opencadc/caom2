FROM cadc-java:1

RUN dnf install -y wcslib && dnf clean all

ADD build/distributions/caom2-meta-sync.tar /

CMD ["/caom2-meta-sync/bin/caom2-meta-sync"]

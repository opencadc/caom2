FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="0.3.0"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/caom2-artifact-discover.tar /

CMD ["/caom2-artifact-discover/bin/caom2-artifact-discover"]

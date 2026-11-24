FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="0.5.0"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/caom2-artifact-download.tar /

CMD ["/caom2-artifact-download/bin/caom2-artifact-download"]

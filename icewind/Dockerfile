FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="0.4.0"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/icewind.tar /

# increase heap size for non-scalable validate mode
ENV JAVA_HEAP_SIZE="32768m"

CMD ["/icewind/bin/icewind"]

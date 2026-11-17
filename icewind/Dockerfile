FROM images.opencadc.org/library/cadc-java:1

ADD build/distributions/icewind.tar /

# increase heap size for non-scalable validate mode
ENV JAVA_HEAP_SIZE="32768m"

CMD ["/icewind/bin/icewind"]

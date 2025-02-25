FROM images.opencadc.org/library/cadc-java:1

ADD build/distributions/icewind.tar /

# temporary hack: increase heap size for non-scalable validate mode
ENV JAVA_OPTS="-Xms1024m -Xmx32768m -XX:+UseParallelGC"

CMD ["/icewind/bin/icewind"]

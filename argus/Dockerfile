FROM images.opencadc.org/library/cadc-tomcat:1

ENV IMAGE_VERSION="1.0.28"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

COPY build/libs/argus.war /usr/share/tomcat/webapps

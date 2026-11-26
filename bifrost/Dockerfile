FROM images.opencadc.org/library/cadc-tomcat:1

ENV IMAGE_VERSION="0.5.9"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

COPY build/libs/bifrost.war /usr/share/tomcat/webapps

FROM images.opencadc.org/library/cadc-tomcat:1 

ENV IMAGE_VERSION="2.0-BETA-11"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

COPY build/libs/torkeep.war /usr/share/tomcat/webapps

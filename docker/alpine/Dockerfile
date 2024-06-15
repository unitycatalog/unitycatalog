FROM alpine:3.20.0

LABEL version="0.0.1"
LABEL description="First cut of docker image for running UC in a container."

ARG jdkHomeDir=/etc/zulu11-jdk11

ENV JAVA_HOME=$jdkHomeDir
ENV UNITYCATALOG_HOME="/unitycatalog"
ENV PATH="$PATH:$JAVA_HOME/bin:$UNITYCATALOG_HOME/bin"

RUN apk update && apk upgrade && \
    apk add --no-cache git curl bash tar && \
    git clone https://github.com/unitycatalog/unitycatalog.git && \
    curl -L https://cdn.azul.com/zulu/bin/zulu11.72.19-ca-jdk11.0.23-linux_musl_x64.tar.gz -o zulu11-jdk11.tar.gz && \
    mkdir $jdkHomeDir && \
    tar xvfz zulu11-jdk11.tar.gz --strip-components=1 -C $jdkHomeDir  && \
    rm zulu11-jdk11.tar.gz && \
    cd /unitycatalog && \
    ./build/sbt -info clean package

WORKDIR /unitycatalog

EXPOSE 8080

ENTRYPOINT ["start-uc-server"]
CMD ["8080"]

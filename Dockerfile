FROM openjdk:11

COPY . /usr/src/myapp
WORKDIR /usr/src/myapp

RUN build/sbt clean compile

CMD ["bin/start-uc-server"]


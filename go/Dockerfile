FROM golang:1.17.0

COPY . /tmp/build

RUN cd /tmp/build \
  && go build -o run main.go \
  && cp run /usr/local/bin

ENTRYPOINT ["/usr/local/bin/run"]

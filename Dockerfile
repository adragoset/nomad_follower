FROM golang:1.10.0-alpine3.7

RUN apk add --update --no-cache git

ADD . /go/src/github.com/adragoset/nomad_follower

RUN set -ex \
    && go get github.com/kardianos/govendor \
    && cd /go/src/github.com/adragoset/nomad_follower \
    && govendor install \
    && go install

CMD nomad_follower
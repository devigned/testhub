FROM iron/go:dev

ENV SRC_DIR=src/github.com/devigned/testhub/
ADD . $SRC_DIR
RUN rm -rf $SRC_DIR/bin $SRC_DIR/output
WORKDIR $SRC_DIR/
RUN go get -u github.com/golang/dep/cmd/dep && dep ensure && make

FROM iron/go:1.10.2
WORKDIR /
COPY --from=0 /go/src/github.com/devigned/testhub/bin/testhub .
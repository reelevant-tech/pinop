FROM golang:1.15.6-alpine3.12 AS build

WORKDIR /root
COPY src ./src
COPY go.mod .
COPY go.sum .
RUN go build -o /tmp/pinop src/*.go

FROM alpine:3.12  
RUN apk --no-cache add ca-certificates
COPY --from=build /tmp/pinop /usr/local/sbin/pinop
RUN chmod +x /usr/local/sbin/pinop
CMD ["pinop"]  
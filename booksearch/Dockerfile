FROM golang:1.14.2

RUN adduser --disabled-password --gecos '' api
USER api

WORKDIR /go/src/project
COPY . .

RUN go install -v ./...

CMD ["go", "run", "main.go"]

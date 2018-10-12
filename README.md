# golang-upload-s3
Concurrent multipart upload to AWS S3

## Build

First, get golang-upload-s3 code:

go get github.com/viktorburka/golang-upload-s3

Then go to your GOPATH directory:

cd $GOPATH/src/github.com/viktorburka/golang-upload-s3

Pull the dependencies:

go get -d -v ./...

And build it:

go build -v ./...

You should be able to find golang-upload-s3 executable in $GOPATH/bin directory

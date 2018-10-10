package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"os"
	"path/filepath"
	"sort"
)

var f = flag.String("f", "", "file to upload")
var b = flag.String("b", "", "s3 bucket name")
var c = flag.Int("c", 10, "cache size in MB")

const usage = `Usage: golang-upload-s3 -f <file to upload> -b <s3 bucket name> [-c <cache size in MB>]`

func main()  {

	// process command line arguments
	flag.Parse()
	if len(*f) == 0 || len(*b) == 0 {
		fmt.Printf("Invalid command line arguments.\n%v\n", usage)
		os.Exit(-1)
	}

	// upload
	uploadConcurrent(*f, *b)
}

// Performs concurrent multipart upload
// of localPath to S3 bucket named bucket.
func uploadConcurrent(localPath string, bucket string)  {

	const maxWorkers = 5

	// helper function to allocate int64 and
	// initialize it in one function call
	var newInt64 = func(init int64) *int64 {
		val := new(int64)
		*val = init
		return val
	}

	var partNumber int64 = 1 // part number must start from 1 according to AWS SDK
	var totalBytes int64

	keyName := filepath.Base(localPath) // extract file name from the path and consider it as key name

	s, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	s3client := s3.New(s)

	mpuInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
	}

	// initiate multipart upload
	mpu, err := s3client.CreateMultipartUpload(mpuInput)
	if err != nil {
		printAwsError(err)
		panic(err)
	}

	file, err := os.Open(localPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// etags will store multipart upload Etag values
	// for proper completion of multipart upload
	etags       := make([]*s3.CompletedPart, 0)

	etagschan   := make(chan *s3.CompletedPart)
	workersCtrl := make(chan struct{}, maxWorkers)
	endRead     := make(chan struct{})
	endUpload   := make(chan struct{})

	go func() {
		var cleanup = func() {
			close(workersCtrl)
			close(etagschan)
			close(endRead)
			close(endUpload)
		}
		defer cleanup()
		done := false
		for {
			select {
			case etag := <-etagschan:
				etags = append(etags, etag)
				<-workersCtrl
				if done && len(workersCtrl) == 0 {
					goto End
				}
			case <-endRead:
				done = true
				if len(workersCtrl) == 0 {
					goto End
				}
			}
		}
	End:
		endUpload <- struct{}{}
	}()

	fmt.Println("Uploading...")
	for {
		buf := make([]byte, 1024 * 1024 * (*c))
		bytesRead, err := file.Read(buf)
		if err == io.EOF {
			// io.EOF always returns bytesRead = 0 so we are safe to break
			endRead <- struct{}{}
			break
		}
		if err != nil {
			panic(err)
		}

		// increment workers count toward max
		workersCtrl <- struct{}{}

		go func(buf []byte, pn int64) {
			bufReader := bytes.NewReader(buf)
			input := &s3.UploadPartInput{
				Body:       bufReader,
				Bucket:     aws.String(bucket),
				Key:        aws.String(keyName),
				PartNumber: aws.Int64(pn),
				UploadId:   mpu.UploadId,
			}
			result, err := s3client.UploadPart(input)
			if err != nil {
				printAwsError(err)
				panic(err)
			}
			etagschan <- &s3.CompletedPart{ ETag:result.ETag, PartNumber: newInt64(pn) }
			totalBytes += bufReader.Size()
		}(buf[0:bytesRead], partNumber)  // bytes read, not buffer size

		partNumber++
	}

	<-endUpload

	sort.Slice(etags, func(i, j int) bool {
		return *etags[i].PartNumber < *etags[j].PartNumber
	})

	cmpuInput := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: etags,
		},
		UploadId: mpu.UploadId,
	}

	result, err := s3client.CompleteMultipartUpload(cmpuInput)
	if err != nil {
		printAwsError(err)
		panic(err)
	}

	fmt.Println("Successfully uploaded object", *result.Key, "to bucket. Etag:", *result.Bucket, *result.ETag)
}

func printAwsError(err error) {
	if aerr, ok := err.(awserr.Error); ok {
		fmt.Printf("%v (code: %v)\n", aerr.Error(), aerr.Code())
	} else {
		fmt.Println(err.Error())
	}
}

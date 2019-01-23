# interceptd-api-integrator

AWS Lambda written in Go consuming SNS events triggered from S3.

## Getting Started

Interceptd-api-integrator consumes SNS events. That's why, presumably you create bucket and connect necessary SNS topics to this bucket's events.

### Prerequisites

You should install Go in your local computer to compile and set GOPATH variable appropriately. 

When you create AWS Lambda function, you have create with these properties

```
Runtime: Go 1.x
Handler: main
Environment Variables: BULK_COUNT={some integer value like 10}, URL={value that get request will be completed}
```

### Installing

After you create AWS Lambda function with proper values. You compile source code and zip in order to upload it.

For Linux:

```
go build -o main main.go
zip main.zip main
```

If you are using Mac:

```
GOOS=linux GOARCH=amd64 go build -o main main.go
zip main.zip main
```

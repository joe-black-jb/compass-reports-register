FROM golang:1.23.2 as build
WORKDIR /app

# Copy dependencies list
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Build with optional lambda.norpc tag
COPY . .

# RUN go build -tags lambda.norpc -o tmp/main ./cmd/compass-api
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tmp/main .

# Copy artifacts to a clean image
FROM public.ecr.aws/lambda/provided:al2023

COPY --from=build /app/tmp/main ./main
ENTRYPOINT [ "./main" ]


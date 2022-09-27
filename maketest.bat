go build
go install . 
protoc --proto_path=. --proto_path=./proto --go_out=paths=source_relative:. --go-asynqgen_out=paths=source_relative:.\ test/example.proto
@rem protoc --proto_path=. --proto_path=./proto --go_out=paths=source_relative:. proto/asynq.proto
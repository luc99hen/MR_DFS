rm -rf ./TinyDFS/*

docker-compose stop
docker build -t go-test .
# docker-compose up -d


rm -rf ./TinyDFS/*
rm -f local-*

docker-compose stop
docker build -t go-test .
# docker-compose up -d


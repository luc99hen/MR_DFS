upgrade proposal:
- 添加更新文件功能 update， append(线程安全)
- Client通过NN读取相关信息并通过NN与DN交互通信。 => (GFS)版本，Client尽量减少与NN的交互减轻NN的压力。
    - put 接口已连通（大文件优化待实现）
- 利用Log日志做replay

## docker usage

- `docker build -t go-test .`
- `docker run  --expose 11091 -dp 11091:11091 go-test go run /dfs/DN1.go`


[docker for go](https://www.callicoder.com/docker-golang-image-container-example/)
[docker volume](https://www.jianshu.com/p/ef0f24fd0674)
[docker tutorial from ms](https://docs.microsoft.com/en-us/visualstudio/docker/tutorials/use-docker-compose)

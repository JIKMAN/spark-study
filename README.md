## Spark-study

* Run Zeppelin in Docker container

```bash
docker run -p 4040:4040 -p 8080:8080 --privileged=true -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_NOTEBOOK_DIR='/notebook' -e ZEPPELIN_LOG_DIR='/logs' -d apache/zeppelin:0.8.1 /zeppelin/bin/zeppelin.sh
```

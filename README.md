First copy [`flink-conf.yaml`](flink-conf.yaml) to `"$FLINK_HOME/conf"`

Build jar with:

```shell
./gradlew shadowJar
```

Then:

```shell
"$FLINK_HOME"/bin/start-cluster.sh
"$FLINK_HOME"/bin/flink run -p 1 build/libs/test-flink-state-processor-shadow.jar
"$FLINK_HOME"/bin/stop-cluster.sh
rm -f "$FLINK_HOME/log"/*
```

If the job takes more than 1 minute,
the issue was reproduced,
otherwise repeat with a fresh cluster.

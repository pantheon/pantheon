## Configuration of Prometheus integration:

**JMX exporter** ([https://github.com/prometheus/jmx_exporter](https://github.com/prometheus/jmx_exporter))
(java agent that runs on the same JVM with Pantheon app and exposes JMX metrics over HTTP).  

a) Download the jmx exporter jar:

```
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.3.1/jmx_prometheus_javaagent-0.3.1.jar -O conf/jmx_prometheus_javaagent.jar
```

b) Edit the file `conf/metrics.yaml` if required.

c) Lauch Pantheon with the following `JAVA_OPTS` envvar

```
export JAVA_OPTS="$JAVA_OPTS -javaagent:conf/jmx_prometheus_javaagent.jar=8080:conf/metrics.yaml -Dcom.sun.management.jmxremote.port=5000 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
```

d) Ensure slick is initialized by calling an appropriate endpoint, e.g. [http://localhost:4300/DataSourceProducts](http://localhost:4300/dataSourceProducts)   

e) Fetch metrics via [http://localhost:8080/](http://localhost:8080/) and check that metrics prefixed with 'com.zaxxer.hikari' and 'slick' are present along with many other basic JVM stats.  

**Prometheus server:**

Follow the instructions to launch Prometheus server (make the 'targets' socket in config correspond to the socket of JMX exporter web endpoint):

 [https://prometheus.io/docs/prometheus/latest/getting_started/](https://prometheus.io/docs/prometheus/latest/getting_started/)


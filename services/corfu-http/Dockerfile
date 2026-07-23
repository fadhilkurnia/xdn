# HTTP KV frontend sidecar for the corfu cluster reference service. Based on
# the corfu-server image so the runtime jars and a JDK are available; the
# frontend is the corfu CLIENT, compiled at build time against the shipped
# shaded jars. See Frontend.java for the API surface.
# Build: docker build -t xdn-corfu-http:test services/corfu-http/
FROM corfudb/corfu-server:0.9.2.0-SNAPSHOT
COPY Frontend.java /frontend/Frontend.java
RUN cd /frontend && javac -cp "/usr/share/corfu/lib/*" Frontend.java
ENTRYPOINT ["/bin/sh", "-c", "exec java -cp '/usr/share/corfu/lib/*:/frontend' -XX:+UseG1GC -Djava.io.tmpdir=/tmp Frontend"]

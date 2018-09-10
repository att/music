#registry.hub.docker.com/
FROM library/cassandra:3.0
RUN mkdir -p /docker-entrypoint-initdb.d && mkdir -p /home/cassandra/.cassandra && chown -R cassandra /home/cassandra
COPY cassandra.yaml /etc/cassandra/
COPY music_single.cql /docker-entrypoint-initdb.d/
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh 
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh && chown -R cassandra /docker-entrypoint-initdb.d

ENTRYPOINT ["docker-entrypoint.sh"]

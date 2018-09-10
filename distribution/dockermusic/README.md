### Docker Setup for Single instance of MUSIC

<p>Please update the <b>properties/music.properties</b> file to fit your env.<br/>
Update the music.sh file.<br/>
The beginning of the <b>music.sh</b> file contains various variables.<br/></p>
NEXUS_DOCKER_REPO - default REPO - Will read /opt/config/nexus_docker_repo.txt if other is needed.<br/>
CASS_IMG - Cassandra Image<br/>
TOMCAT_IMG - Tomcat Image<br/>
ZK_IMG - Zookeeper Image<br/>
MUSIC_IMG - Music Image containing the MUSIC war file.<br/>
WORK_DIR -  Default to PWD.<br/>
CASS_USERNAME - Username for Cassandra - should match cassandra.user in music.properties 
file<br/>
CASS_PASSWORD - Password for Cassandra - should match cassandra.password in music.properties.<br/>

MUSIC Logs will be saved in logs/MUSIC after start of tomcat.<br/> 

```bash
# Start containers
./music.sh start
# Stop containers
./music.sh stop
```

If you want to check out Cassandra db with cqlsh.
```bash
docker exec –it music-db bash
#at the prompt youcan run cqlsh as:
cqlsh –u <user> -p <password>
```

Zookeeper:

```bash
docker exec –it music-zk bash
#and then run:
zkCli.sh
```

For other logs do <br/>
```bash
docker logs music-tomcat (tomcat)<br/> 
```
to have rolling logs use –f as docker logs –f music-tomcat<br/>
```bash
docker logs music-zk   (zookeeper)<br/>
docker logs music-db  (Cassandra )<br/> 
```
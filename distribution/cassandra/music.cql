CREATE KEYSPACE admin
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor': 1
    }
    AND DURABLE_WRITES = true;
 
CREATE TABLE admin.keyspace_master (
    uuid uuid,
    keyspace_name text,
    application_name text,
    is_api boolean,
    password text,
    username text,
    is_aaf boolean,
    PRIMARY KEY (uuid)
);

CREATE USER nelson24 WITH PASSWORD ‘winman123’ SUPERUSER;
ALTER USER cassandra WITH PASSWORD ‘SomeLongRandomStringNoonewillthinkof’;

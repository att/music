CREATE KEYSPACE IF NOT EXISTS admin
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor': 1
    }
    AND DURABLE_WRITES = true;
 
CREATE TABLE IF NOT EXISTS admin.keyspace_master (
    uuid uuid,
    keyspace_name text,
    application_name text,
    is_api boolean,
    password text,
    username text,
    is_aaf boolean,
    PRIMARY KEY (uuid)
);

CREATE ROLE IF NOT EXISTS CASSUSER WITH PASSWORD = 'CASSPASS' AND SUPERUSER = True AND LOGIN = True;
ALTER ROLE cassandra WITH PASSWORD = 'SomeLongRandomStringNoonewillthinkof';

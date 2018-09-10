 `For Single install:`_

 `Multi-Site Install:`_

 `Headers:`_

 `AAF Authentication`_

 `AID Authentication Non-AAF`_

`Onboarding API`_

`Add Application`_

`Get Application`_

`Edit Application`_

`Delete Application`_


Steps to test AAF MUSIC has been enhanced to support applications which are already authenticated using AAF and applications which are not authenticated using AAF.

If an application has already been using AAF, it should have required namespace, userId and password.

**Non AAF applications (AID)** Works just like AAF but Namespace is an app name and MUSIC manages the User instead of AAF

All the required params should be sent as headers.

Changed in Cassandra: Admin needs to create the following keyspace and table.

In the cassandra bin dir run ./cqlsh and log in to db then:

If you want to save the following in a file you can then run ./cqlsh -f <file.cql>

For Single install:
^^^^^^^^^^^^^^^^^^^
::

  //Create Admin Keyspace
   
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


Multi-Site Install:
^^^^^^^^^^^^^^^^^^^

::

  //Create Admin Keyspace
 
  CREATE KEYSPACE admin
  WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    'DC1':2
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

Headers:
^^^^^^^^

For AAF applications all the 3 headers ns, userId and password are mandatory.

For Non AAF applications if aid is not provided MUSIC creates new random unique UUID and returns to caller.

Caller application then need to save the UUID and need to pass the UUID to further modify/access the keyspace.

Required Headers

AAF Authentication
^^^^^^^^^^^^^^^^^^
::

  Key     : Value        : Description 
  ns      : org.onap.aaf : AAF Namespace
  userId  : username     : USer Id
  password: password     : Password of User

AID Authentication Non-AAF
^^^^^^^^^^^^^^^^^^^^^^^^^^

::

  Key     : Value        : Description 
  ns      : App Name     : App Name
  userId  : username     : Username for this user (Required during Create keyspace Only)
  password: password     : Password for this user (Required during Create keyspace Only)

Onboarding API
^^^^^^^^^^^^^^

Add Application
^^^^^^^^^^^^^^^

::

  POST URL: /MUSIC/rest/v2/admin/onboardAppWithMusic  with JSON as follows:

  {
   "appname": "<the Namespace for aaf or the Identifier for the specific app using AID access",
   "userId" : "<userid>",
   "isAAF"  : true/false,
   "password" : ""
 }
  
Get Application
^^^^^^^^^^^^^^^

::

  POST URL: /MUSIC/rest/v2/admin/search  with JSON as follows:

  {
   "appname": "<the Namespace for aaf or the Identifier for the specific app using AID access",
   "isAAF"  : true/false,
   "aid" : "Unique ID for this user"
  }
  
Edit Application
^^^^^^^^^^^^^^^^

::

  PUT URL: /MUSIC/rest/v2/admin/onboardAppWithMusic  with JSON as follows: 

  {
  "aid" : "Unique ID for this user",
  "appname": "<the Namespace for aaf or the Identifier for the specific app using AID access",
  "userId" : "<userid>",
  "isAAF"  : true/false,
  "password" : ""
  }
  
Delete Application
^^^^^^^^^^^^^^^^^^

::

  DELETE URL: /MUSIC/rest/v2/admin/onboardAppWithMusic  with JSON as follows:

 {
 "aid" : "Unique ID for this app"
 }

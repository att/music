
# Multi-site Coordination for Replicated Services
-------------------------------------------------

The complexity of replicated, multi-site
distributed applications brings forth the need for rich distribution coordination patterns to manage
these applications. We contend that, to build such patterns, it is necessary to tightly integrate
coordination primitives such as mutual exclusion and barriers with state-management in these
replicated systems. This is easier said than done, since coordination primitives typically need
strong consistency that may render them unavailable during partitions. On the other hand, the
relative ubiquity of network partitions and large WAN latencies in a multi-site setting dictate that
replicated state is usually maintained in an eventually consistent store. We address this conflict
by presenting a MUlti-SIte Coordination service (MUSIC), that combines a strongly consistent locking
service with an eventually consistent state store to provide abstractions that enable rich
distributed coordination on shared state, as and when required.

This is the repository for MUSIC.  The pom.xml corresponds to the rest-based version of MUSIC. 

## MUSIC Logging through log4j
------------------------------------------
This section explains how MUSIC log4j properties can be used and modified to control logging. 

Once MUSIC.war is installed, tomcat7 will unpack it into /var/lib/tomcat7/webapps/MUSIC (this is the
standard Ubuntu installation, the location may differ for self installs).

Look at /var/lib/tomcat7/webapps/MUSIC/WEB-INF/log4j.properties:

```properties
   # Root logger option
   log4j.rootLogger=INFO, file, stdout

   # Direct log messages to a log file
   log4j.appender.file=org.apache.log4j.RollingFileAppender
   log4j.appender.file.File=${catalina.home}/logs/music.log
   log4j.appender.file.MaxFileSize=10MB
   log4j.appender.file.MaxBackupIndex=10
   log4j.appender.file.layout=org.apache.log4j.PatternLayout
   log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

   # Direct log messages to stdout
   log4j.appender.stdout=org.apache.log4j.ConsoleAppender
   log4j.appender.stdout.Target=System.out
   log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
   log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
```

Notice there are two log4j.appender sections. The first one directs log lines to a file. The second
one directs log lines to stdout (which winds up in catalina.out).

To redirect MUSIC's log info to a log file, with more control over rotation rules:

1. Set "log4j.rootLogger" to "INFO, file" instead. (INFO can also be changed. See the
log4j.properties docs for info.)
2. Uncomment all the lines under "# Direct log messages to a log file".
3. Fix the line with "og4j" so that it reads "log4j" instead.
4. Change "${catalina.home}/logs/music.log" to "/var/log/music/music.log"
5. Adjust "MaxFileSize" to the largest size desired for each log file prior to rotation.
6. Adjust "MaxBackupIndex" to the max number of desired rotated logs.
7. Remove any unwanted files from /var/log/tomcat7.
8. Restart tomcat7 with "service tomcat7 restart".
9. Backup this file. When updating MUSIC.war, it might be overwritten and need to be replaced
(followed by bouncing tomcat7 again).

Note that the logrotate.d settings for tomcat7 may stay in place (for catalina.out). In the case of
MUSIC, logrotate.d may not run often enough for the file to be
rotated before running out of disk space. It's expected that using log4j's rotation in conjunction
with a separate log file will help alleviate filesystem pressure.

More info about log4j.properties:

https://docs.oracle.com/cd/E29578_01/webhelp/cas_webcrawler/src/cwcg_config_log4j_file.html

MUSIC uses log4j 1.2.17 which is EOL. Music will be changing to 2.x, at which point this
file's syntax will change significantly (new info will be sent at that time).

## Muting MUSIC jersey output
----------------------------

The jersey package that MUSIC uses to parse REST calls prints out the entire header and json body by
default. To mute it, please remove the following lines from the web.xml in the WEB_INF foler:

```xml
<init-param>
  <param-name>com.sun.jersey.spi.container.ContainerResponseFilters</param-name>
  <param-value>com.sun.jersey.api.container.filter.LoggingFilter</param-value>
</init-param>
```

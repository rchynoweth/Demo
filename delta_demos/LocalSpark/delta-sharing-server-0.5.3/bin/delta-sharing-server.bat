@REM delta-sharing-server launcher script
@REM
@REM Environment:
@REM JAVA_HOME - location of a JDK home dir (optional if java on path)
@REM CFG_OPTS  - JVM options (optional)
@REM Configuration:
@REM DELTA_SHARING_SERVER_config.txt found in the DELTA_SHARING_SERVER_HOME.
@setlocal enabledelayedexpansion
@setlocal enableextensions

@echo off


if "%DELTA_SHARING_SERVER_HOME%"=="" (
  set "APP_HOME=%~dp0\\.."

  rem Also set the old env name for backwards compatibility
  set "DELTA_SHARING_SERVER_HOME=%~dp0\\.."
) else (
  set "APP_HOME=%DELTA_SHARING_SERVER_HOME%"
)

set "APP_LIB_DIR=%APP_HOME%\lib\"

rem Detect if we were double clicked, although theoretically A user could
rem manually run cmd /c
for %%x in (!cmdcmdline!) do if %%~x==/c set DOUBLECLICKED=1

rem FIRST we load the config file of extra options.
set "CFG_FILE=%APP_HOME%\DELTA_SHARING_SERVER_config.txt"
set CFG_OPTS=
call :parse_config "%CFG_FILE%" CFG_OPTS

rem We use the value of the JAVA_OPTS environment variable if defined, rather than the config.
set _JAVA_OPTS=%JAVA_OPTS%
if "!_JAVA_OPTS!"=="" set _JAVA_OPTS=!CFG_OPTS!

rem We keep in _JAVA_PARAMS all -J-prefixed and -D-prefixed arguments
rem "-J" is stripped, "-D" is left as is, and everything is appended to JAVA_OPTS
set _JAVA_PARAMS=
set _APP_ARGS=

set "APP_CLASSPATH=%APP_LIB_DIR%\io.delta.delta-sharing-server-0.5.3.jar;%APP_LIB_DIR%\org.scala-lang.scala-library-2.12.10.jar;%APP_LIB_DIR%\com.thesamet.scalapb.scalapb-runtime_2.12-0.11.1.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-core-2.6.7.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-databind-2.6.7.3.jar;%APP_LIB_DIR%\com.fasterxml.jackson.module.jackson-module-scala_2.12-2.6.7.1.jar;%APP_LIB_DIR%\com.fasterxml.jackson.dataformat.jackson-dataformat-yaml-2.6.7.jar;%APP_LIB_DIR%\org.json4s.json4s-jackson_2.12-3.5.3.jar;%APP_LIB_DIR%\com.linecorp.armeria.armeria-scalapb_2.12-1.6.0.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-aws-2.10.1.jar;%APP_LIB_DIR%\com.amazonaws.aws-java-sdk-bundle-1.12.189.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-azure-2.10.1.jar;%APP_LIB_DIR%\com.google.cloud.google-cloud-storage-2.2.2.jar;%APP_LIB_DIR%\com.google.cloud.bigdataoss.gcs-connector-hadoop2-2.2.4.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-common-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-client-2.10.1.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-hadoop-1.10.1.jar;%APP_LIB_DIR%\io.delta.delta-standalone_2.12-0.4.0.jar;%APP_LIB_DIR%\org.apache.spark.spark-sql_2.12-2.4.7.jar;%APP_LIB_DIR%\org.slf4j.slf4j-api-1.7.30.jar;%APP_LIB_DIR%\org.slf4j.slf4j-simple-1.6.1.jar;%APP_LIB_DIR%\net.sourceforge.argparse4j.argparse4j-0.9.0.jar;%APP_LIB_DIR%\com.thesamet.scalapb.lenses_2.12-0.11.1.jar;%APP_LIB_DIR%\com.google.protobuf.protobuf-java-3.19.1.jar;%APP_LIB_DIR%\org.scala-lang.modules.scala-collection-compat_2.12-2.4.3.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-annotations-2.6.7.jar;%APP_LIB_DIR%\org.scala-lang.scala-reflect-2.12.10.jar;%APP_LIB_DIR%\com.fasterxml.jackson.module.jackson-module-paranamer-2.7.9.jar;%APP_LIB_DIR%\org.yaml.snakeyaml-1.15.jar;%APP_LIB_DIR%\org.json4s.json4s-core_2.12-3.5.3.jar;%APP_LIB_DIR%\com.linecorp.armeria.armeria-1.6.0.jar;%APP_LIB_DIR%\com.thesamet.scalapb.scalapb-json4s_2.12-0.11.0.jar;%APP_LIB_DIR%\com.google.code.findbugs.jsr305-3.0.2.jar;%APP_LIB_DIR%\com.linecorp.armeria.armeria-grpc-1.6.0.jar;%APP_LIB_DIR%\com.thesamet.scalapb.scalapb-runtime-grpc_2.12-0.11.1.jar;%APP_LIB_DIR%\org.apache.commons.commons-lang3-3.5.jar;%APP_LIB_DIR%\org.apache.httpcomponents.httpclient-4.5.13.jar;%APP_LIB_DIR%\com.microsoft.azure.azure-storage-7.0.1.jar;%APP_LIB_DIR%\org.codehaus.jackson.jackson-mapper-asl-1.9.13.jar;%APP_LIB_DIR%\org.codehaus.jackson.jackson-core-asl-1.9.13.jar;%APP_LIB_DIR%\com.google.guava.guava-31.0.1-jre.jar;%APP_LIB_DIR%\com.google.guava.failureaccess-1.0.1.jar;%APP_LIB_DIR%\com.google.guava.listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar;%APP_LIB_DIR%\org.checkerframework.checker-qual-3.19.0.jar;%APP_LIB_DIR%\com.google.j2objc.j2objc-annotations-1.3.jar;%APP_LIB_DIR%\com.google.http-client.google-http-client-1.40.1.jar;%APP_LIB_DIR%\io.opencensus.opencensus-contrib-http-util-0.28.0.jar;%APP_LIB_DIR%\com.google.http-client.google-http-client-jackson2-1.40.1.jar;%APP_LIB_DIR%\com.google.api-client.google-api-client-1.32.2.jar;%APP_LIB_DIR%\com.google.oauth-client.google-oauth-client-1.32.1.jar;%APP_LIB_DIR%\com.google.http-client.google-http-client-gson-1.40.1.jar;%APP_LIB_DIR%\com.google.http-client.google-http-client-apache-v2-1.40.1.jar;%APP_LIB_DIR%\com.google.apis.google-api-services-storage-v1-rev20211201-1.32.1.jar;%APP_LIB_DIR%\com.google.code.gson.gson-2.8.9.jar;%APP_LIB_DIR%\com.google.cloud.google-cloud-core-2.3.3.jar;%APP_LIB_DIR%\com.google.auto.value.auto-value-annotations-1.8.2.jar;%APP_LIB_DIR%\com.google.api.grpc.proto-google-common-protos-2.7.0.jar;%APP_LIB_DIR%\com.google.cloud.google-cloud-core-http-2.3.3.jar;%APP_LIB_DIR%\com.google.http-client.google-http-client-appengine-1.40.1.jar;%APP_LIB_DIR%\com.google.api.gax-httpjson-0.92.1.jar;%APP_LIB_DIR%\com.google.api.gax-2.7.1.jar;%APP_LIB_DIR%\com.google.auth.google-auth-library-credentials-1.3.0.jar;%APP_LIB_DIR%\com.google.auth.google-auth-library-oauth2-http-1.3.0.jar;%APP_LIB_DIR%\com.google.api.api-common-2.1.1.jar;%APP_LIB_DIR%\javax.annotation.javax.annotation-api-1.3.2.jar;%APP_LIB_DIR%\io.opencensus.opencensus-api-0.28.0.jar;%APP_LIB_DIR%\io.grpc.grpc-context-1.42.1.jar;%APP_LIB_DIR%\com.google.api.grpc.proto-google-iam-v1-1.1.7.jar;%APP_LIB_DIR%\com.google.protobuf.protobuf-java-util-3.19.1.jar;%APP_LIB_DIR%\org.threeten.threetenbp-1.5.2.jar;%APP_LIB_DIR%\com.google.api-client.google-api-client-jackson2-1.32.2.jar;%APP_LIB_DIR%\com.google.cloud.bigdataoss.util-2.2.4.jar;%APP_LIB_DIR%\com.google.cloud.bigdataoss.util-hadoop-hadoop2-2.2.4.jar;%APP_LIB_DIR%\com.google.cloud.bigdataoss.gcsio-2.2.4.jar;%APP_LIB_DIR%\com.google.flogger.flogger-0.7.1.jar;%APP_LIB_DIR%\com.google.flogger.google-extensions-0.7.1.jar;%APP_LIB_DIR%\com.google.flogger.flogger-system-backend-0.7.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-annotations-2.10.1.jar;%APP_LIB_DIR%\commons-cli.commons-cli-1.2.jar;%APP_LIB_DIR%\org.apache.commons.commons-math3-3.4.1.jar;%APP_LIB_DIR%\xmlenc.xmlenc-0.52.jar;%APP_LIB_DIR%\commons-codec.commons-codec-1.11.jar;%APP_LIB_DIR%\commons-io.commons-io-2.4.jar;%APP_LIB_DIR%\commons-net.commons-net-3.1.jar;%APP_LIB_DIR%\commons-collections.commons-collections-3.2.2.jar;%APP_LIB_DIR%\javax.servlet.servlet-api-2.5.jar;%APP_LIB_DIR%\org.mortbay.jetty.jetty-6.1.26.jar;%APP_LIB_DIR%\org.mortbay.jetty.jetty-util-6.1.26.jar;%APP_LIB_DIR%\org.mortbay.jetty.jetty-sslengine-6.1.26.jar;%APP_LIB_DIR%\javax.servlet.jsp.jsp-api-2.1.jar;%APP_LIB_DIR%\com.sun.jersey.jersey-core-1.9.jar;%APP_LIB_DIR%\com.sun.jersey.jersey-json-1.9.jar;%APP_LIB_DIR%\com.sun.jersey.jersey-server-1.9.jar;%APP_LIB_DIR%\commons-logging.commons-logging-1.2.jar;%APP_LIB_DIR%\log4j.log4j-1.2.17.jar;%APP_LIB_DIR%\net.java.dev.jets3t.jets3t-0.9.0.jar;%APP_LIB_DIR%\commons-lang.commons-lang-2.6.jar;%APP_LIB_DIR%\commons-configuration.commons-configuration-1.6.jar;%APP_LIB_DIR%\commons-digester.commons-digester-1.8.jar;%APP_LIB_DIR%\commons-beanutils.commons-beanutils-1.9.4.jar;%APP_LIB_DIR%\org.slf4j.slf4j-log4j12-1.7.25.jar;%APP_LIB_DIR%\org.apache.avro.avro-1.8.2.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-auth-2.10.1.jar;%APP_LIB_DIR%\com.jcraft.jsch-0.1.55.jar;%APP_LIB_DIR%\org.apache.curator.curator-client-2.13.0.jar;%APP_LIB_DIR%\org.apache.curator.curator-recipes-2.13.0.jar;%APP_LIB_DIR%\org.apache.htrace.htrace-core4-4.1.0-incubating.jar;%APP_LIB_DIR%\org.apache.zookeeper.zookeeper-3.4.14.jar;%APP_LIB_DIR%\org.apache.commons.commons-compress-1.19.jar;%APP_LIB_DIR%\org.codehaus.woodstox.stax2-api-3.1.4.jar;%APP_LIB_DIR%\com.fasterxml.woodstox.woodstox-core-5.0.3.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-hdfs-client-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-mapreduce-client-app-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-api-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-mapreduce-client-core-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-mapreduce-client-jobclient-2.10.1.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-column-1.10.1.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-format-2.4.0.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-jackson-1.10.1.jar;%APP_LIB_DIR%\org.xerial.snappy.snappy-java-1.1.7.5.jar;%APP_LIB_DIR%\commons-pool.commons-pool-1.6.jar;%APP_LIB_DIR%\com.github.mjakubowski84.parquet4s-core_2.12-1.2.1.jar;%APP_LIB_DIR%\com.univocity.univocity-parsers-2.7.3.jar;%APP_LIB_DIR%\org.apache.spark.spark-sketch_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-core_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-catalyst_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-tags_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.orc.orc-core-1.5.5-nohive.jar;%APP_LIB_DIR%\org.apache.orc.orc-mapreduce-1.5.5-nohive.jar;%APP_LIB_DIR%\org.apache.arrow.arrow-vector-0.10.0.jar;%APP_LIB_DIR%\org.apache.xbean.xbean-asm6-shaded-4.8.jar;%APP_LIB_DIR%\org.spark-project.spark.unused-1.0.0.jar;%APP_LIB_DIR%\com.thoughtworks.paranamer.paranamer-2.8.jar;%APP_LIB_DIR%\org.json4s.json4s-ast_2.12-3.5.3.jar;%APP_LIB_DIR%\org.json4s.json4s-scalap_2.12-3.5.3.jar;%APP_LIB_DIR%\org.scala-lang.modules.scala-xml_2.12-1.0.6.jar;%APP_LIB_DIR%\io.micrometer.micrometer-core-1.6.5.jar;%APP_LIB_DIR%\io.netty.netty-transport-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-http2-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-haproxy-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-resolver-dns-4.1.63.Final.jar;%APP_LIB_DIR%\org.reactivestreams.reactive-streams-1.0.3.jar;%APP_LIB_DIR%\io.netty.netty-resolver-dns-native-macos-4.1.63.Final-osx-x86_64.jar;%APP_LIB_DIR%\io.netty.netty-transport-native-unix-common-4.1.63.Final-linux-x86_64.jar;%APP_LIB_DIR%\io.netty.netty-transport-native-epoll-4.1.63.Final-linux-x86_64.jar;%APP_LIB_DIR%\io.netty.netty-tcnative-boringssl-static-2.0.38.Final.jar;%APP_LIB_DIR%\io.netty.netty-handler-proxy-4.1.63.Final.jar;%APP_LIB_DIR%\io.grpc.grpc-core-1.41.1.jar;%APP_LIB_DIR%\io.grpc.grpc-protobuf-1.41.1.jar;%APP_LIB_DIR%\io.grpc.grpc-services-1.36.1.jar;%APP_LIB_DIR%\io.grpc.grpc-stub-1.41.1.jar;%APP_LIB_DIR%\org.curioswitch.curiostack.protobuf-jackson-1.2.0.jar;%APP_LIB_DIR%\com.linecorp.armeria.armeria-grpc-protocol-1.6.0.jar;%APP_LIB_DIR%\org.apache.httpcomponents.httpcore-4.4.14.jar;%APP_LIB_DIR%\com.microsoft.azure.azure-keyvault-core-1.0.0.jar;%APP_LIB_DIR%\com.google.errorprone.error_prone_annotations-2.9.0.jar;%APP_LIB_DIR%\com.google.apis.google-api-services-iamcredentials-v1-rev20210326-1.32.1.jar;%APP_LIB_DIR%\io.grpc.grpc-api-1.41.1.jar;%APP_LIB_DIR%\io.grpc.grpc-alts-1.41.1.jar;%APP_LIB_DIR%\io.grpc.grpc-netty-shaded-1.41.1.jar;%APP_LIB_DIR%\com.google.api.grpc.grpc-google-cloud-storage-v2-2.0.1-alpha.jar;%APP_LIB_DIR%\org.checkerframework.checker-compat-qual-2.5.3.jar;%APP_LIB_DIR%\org.codehaus.jettison.jettison-1.1.jar;%APP_LIB_DIR%\com.sun.xml.bind.jaxb-impl-2.2.3-1.jar;%APP_LIB_DIR%\org.codehaus.jackson.jackson-jaxrs-1.9.13.jar;%APP_LIB_DIR%\org.codehaus.jackson.jackson-xc-1.9.13.jar;%APP_LIB_DIR%\asm.asm-3.1.jar;%APP_LIB_DIR%\com.jamesmurty.utils.java-xmlbuilder-0.4.jar;%APP_LIB_DIR%\org.tukaani.xz-1.5.jar;%APP_LIB_DIR%\com.nimbusds.nimbus-jose-jwt-7.9.jar;%APP_LIB_DIR%\org.apache.directory.server.apacheds-kerberos-codec-2.0.0-M15.jar;%APP_LIB_DIR%\org.apache.curator.curator-framework-2.13.0.jar;%APP_LIB_DIR%\com.github.spotbugs.spotbugs-annotations-3.1.9.jar;%APP_LIB_DIR%\org.apache.yetus.audience-annotations-0.5.0.jar;%APP_LIB_DIR%\io.netty.netty-3.10.6.Final.jar;%APP_LIB_DIR%\com.squareup.okhttp.okhttp-2.7.5.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-mapreduce-client-common-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-mapreduce-client-shuffle-2.10.1.jar;%APP_LIB_DIR%\javax.xml.bind.jaxb-api-2.2.2.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-client-2.10.1.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-common-2.10.1.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-common-1.10.1.jar;%APP_LIB_DIR%\org.apache.parquet.parquet-encoding-1.10.1.jar;%APP_LIB_DIR%\com.chuusai.shapeless_2.12-2.3.3.jar;%APP_LIB_DIR%\org.apache.avro.avro-mapred-1.8.2-hadoop2.jar;%APP_LIB_DIR%\com.twitter.chill_2.12-0.9.3.jar;%APP_LIB_DIR%\com.twitter.chill-java-0.9.3.jar;%APP_LIB_DIR%\org.apache.spark.spark-launcher_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-kvstore_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-network-common_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-network-shuffle_2.12-2.4.7.jar;%APP_LIB_DIR%\org.apache.spark.spark-unsafe_2.12-2.4.7.jar;%APP_LIB_DIR%\javax.activation.activation-1.1.1.jar;%APP_LIB_DIR%\javax.servlet.javax.servlet-api-3.1.0.jar;%APP_LIB_DIR%\com.ning.compress-lzf-1.0.3.jar;%APP_LIB_DIR%\org.lz4.lz4-java-1.4.0.jar;%APP_LIB_DIR%\com.github.luben.zstd-jni-1.3.2-2.jar;%APP_LIB_DIR%\org.roaringbitmap.RoaringBitmap-0.7.45.jar;%APP_LIB_DIR%\org.glassfish.jersey.core.jersey-client-2.22.2.jar;%APP_LIB_DIR%\org.glassfish.jersey.core.jersey-common-2.22.2.jar;%APP_LIB_DIR%\org.glassfish.jersey.core.jersey-server-2.22.2.jar;%APP_LIB_DIR%\org.glassfish.jersey.containers.jersey-container-servlet-2.22.2.jar;%APP_LIB_DIR%\org.glassfish.jersey.containers.jersey-container-servlet-core-2.22.2.jar;%APP_LIB_DIR%\com.clearspring.analytics.stream-2.7.0.jar;%APP_LIB_DIR%\io.dropwizard.metrics.metrics-core-3.1.5.jar;%APP_LIB_DIR%\io.dropwizard.metrics.metrics-jvm-3.1.5.jar;%APP_LIB_DIR%\io.dropwizard.metrics.metrics-json-3.1.5.jar;%APP_LIB_DIR%\io.dropwizard.metrics.metrics-graphite-3.1.5.jar;%APP_LIB_DIR%\org.apache.ivy.ivy-2.4.0.jar;%APP_LIB_DIR%\oro.oro-2.0.8.jar;%APP_LIB_DIR%\net.razorvine.pyrolite-4.13.jar;%APP_LIB_DIR%\net.sf.py4j.py4j-0.10.7.jar;%APP_LIB_DIR%\org.apache.commons.commons-crypto-1.0.0.jar;%APP_LIB_DIR%\org.scala-lang.modules.scala-parser-combinators_2.12-1.1.0.jar;%APP_LIB_DIR%\org.codehaus.janino.janino-3.0.16.jar;%APP_LIB_DIR%\org.codehaus.janino.commons-compiler-3.0.16.jar;%APP_LIB_DIR%\org.antlr.antlr4-runtime-4.7.jar;%APP_LIB_DIR%\org.apache.orc.orc-shims-1.5.5.jar;%APP_LIB_DIR%\io.airlift.aircompressor-0.10.jar;%APP_LIB_DIR%\org.apache.arrow.arrow-format-0.10.0.jar;%APP_LIB_DIR%\org.apache.arrow.arrow-memory-0.10.0.jar;%APP_LIB_DIR%\joda-time.joda-time-2.9.9.jar;%APP_LIB_DIR%\com.carrotsearch.hppc-0.7.2.jar;%APP_LIB_DIR%\com.vlkan.flatbuffers-1.2.0-3f79e055.jar;%APP_LIB_DIR%\org.hdrhistogram.HdrHistogram-2.1.12.jar;%APP_LIB_DIR%\org.latencyutils.LatencyUtils-2.0.3.jar;%APP_LIB_DIR%\io.netty.netty-common-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-buffer-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-resolver-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-handler-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-http-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-dns-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-transport-native-unix-common-4.1.63.Final.jar;%APP_LIB_DIR%\io.netty.netty-codec-socks-4.1.63.Final.jar;%APP_LIB_DIR%\com.google.android.annotations-4.1.1.4.jar;%APP_LIB_DIR%\io.perfmark.perfmark-api-0.23.0.jar;%APP_LIB_DIR%\io.grpc.grpc-protobuf-lite-1.41.1.jar;%APP_LIB_DIR%\org.codehaus.mojo.animal-sniffer-annotations-1.19.jar;%APP_LIB_DIR%\jakarta.annotation.jakarta.annotation-api-1.3.5.jar;%APP_LIB_DIR%\net.bytebuddy.byte-buddy-1.10.19.jar;%APP_LIB_DIR%\io.grpc.grpc-auth-1.41.1.jar;%APP_LIB_DIR%\io.grpc.grpc-grpclb-1.41.1.jar;%APP_LIB_DIR%\org.conscrypt.conscrypt-openjdk-uber-2.5.1.jar;%APP_LIB_DIR%\com.google.api.grpc.proto-google-cloud-storage-v2-2.0.1-alpha.jar;%APP_LIB_DIR%\org.mortbay.jetty.servlet-api-2.5-20081211.jar;%APP_LIB_DIR%\com.github.stephenc.jcip.jcip-annotations-1.0-1.jar;%APP_LIB_DIR%\net.minidev.json-smart-2.3.jar;%APP_LIB_DIR%\org.apache.directory.server.apacheds-i18n-2.0.0-M15.jar;%APP_LIB_DIR%\org.apache.directory.api.api-asn1-api-1.0.0-M20.jar;%APP_LIB_DIR%\org.apache.directory.api.api-util-1.0.0-M20.jar;%APP_LIB_DIR%\jline.jline-0.9.94.jar;%APP_LIB_DIR%\com.squareup.okio.okio-1.6.0.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-server-common-2.10.1.jar;%APP_LIB_DIR%\org.fusesource.leveldbjni.leveldbjni-all-1.8.jar;%APP_LIB_DIR%\javax.xml.stream.stax-api-1.0-2.jar;%APP_LIB_DIR%\com.sun.jersey.jersey-client-1.9.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-server-nodemanager-2.10.1.jar;%APP_LIB_DIR%\org.typelevel.macro-compat_2.12-1.1.1.jar;%APP_LIB_DIR%\org.apache.avro.avro-ipc-1.8.2.jar;%APP_LIB_DIR%\com.esotericsoftware.kryo-shaded-4.0.2.jar;%APP_LIB_DIR%\org.roaringbitmap.shims-0.7.45.jar;%APP_LIB_DIR%\javax.ws.rs.javax.ws.rs-api-2.0.1.jar;%APP_LIB_DIR%\org.glassfish.hk2.hk2-api-2.4.0-b34.jar;%APP_LIB_DIR%\org.glassfish.hk2.external.javax.inject-2.4.0-b34.jar;%APP_LIB_DIR%\org.glassfish.hk2.hk2-locator-2.4.0-b34.jar;%APP_LIB_DIR%\org.glassfish.jersey.bundles.repackaged.jersey-guava-2.22.2.jar;%APP_LIB_DIR%\org.glassfish.hk2.osgi-resource-locator-1.0.1.jar;%APP_LIB_DIR%\org.glassfish.jersey.media.jersey-media-jaxb-2.22.2.jar;%APP_LIB_DIR%\javax.validation.validation-api-1.1.0.Final.jar;%APP_LIB_DIR%\net.minidev.accessors-smart-1.2.jar;%APP_LIB_DIR%\com.google.inject.guice-3.0.jar;%APP_LIB_DIR%\com.sun.jersey.contribs.jersey-guice-1.9.jar;%APP_LIB_DIR%\org.apache.hadoop.hadoop-yarn-registry-2.10.1.jar;%APP_LIB_DIR%\org.apache.geronimo.specs.geronimo-jcache_1.0_spec-1.0-alpha-1.jar;%APP_LIB_DIR%\org.ehcache.ehcache-3.3.1.jar;%APP_LIB_DIR%\com.zaxxer.HikariCP-java7-2.4.12.jar;%APP_LIB_DIR%\com.microsoft.sqlserver.mssql-jdbc-6.2.1.jre7.jar;%APP_LIB_DIR%\com.codahale.metrics.metrics-core-3.0.1.jar;%APP_LIB_DIR%\com.esotericsoftware.minlog-1.3.0.jar;%APP_LIB_DIR%\org.objenesis.objenesis-2.5.1.jar;%APP_LIB_DIR%\org.glassfish.hk2.hk2-utils-2.4.0-b34.jar;%APP_LIB_DIR%\org.glassfish.hk2.external.aopalliance-repackaged-2.4.0-b34.jar;%APP_LIB_DIR%\org.javassist.javassist-3.18.1-GA.jar;%APP_LIB_DIR%\org.ow2.asm.asm-5.0.4.jar;%APP_LIB_DIR%\javax.inject.javax.inject-1.jar;%APP_LIB_DIR%\aopalliance.aopalliance-1.0.jar;%APP_LIB_DIR%\..\conf"
set "APP_MAIN_CLASS=io.delta.sharing.server.DeltaSharingService"
set "SCRIPT_CONF_FILE=%APP_HOME%\conf\application.ini"

rem Bundled JRE has priority over standard environment variables
if defined BUNDLED_JVM (
  set "_JAVACMD=%BUNDLED_JVM%\bin\java.exe"
) else (
  if "%JAVACMD%" neq "" (
    set "_JAVACMD=%JAVACMD%"
  ) else (
    if "%JAVA_HOME%" neq "" (
      if exist "%JAVA_HOME%\bin\java.exe" set "_JAVACMD=%JAVA_HOME%\bin\java.exe"
    )
  )
)

if "%_JAVACMD%"=="" set _JAVACMD=java

rem Detect if this java is ok to use.
for /F %%j in ('"%_JAVACMD%" -version  2^>^&1') do (
  if %%~j==java set JAVAINSTALLED=1
  if %%~j==openjdk set JAVAINSTALLED=1
)

rem BAT has no logical or, so we do it OLD SCHOOL! Oppan Redmond Style
set JAVAOK=true
if not defined JAVAINSTALLED set JAVAOK=false

if "%JAVAOK%"=="false" (
  echo.
  echo A Java JDK is not installed or can't be found.
  if not "%JAVA_HOME%"=="" (
    echo JAVA_HOME = "%JAVA_HOME%"
  )
  echo.
  echo Please go to
  echo   http://www.oracle.com/technetwork/java/javase/downloads/index.html
  echo and download a valid Java JDK and install before running delta-sharing-server.
  echo.
  echo If you think this message is in error, please check
  echo your environment variables to see if "java.exe" and "javac.exe" are
  echo available via JAVA_HOME or PATH.
  echo.
  if defined DOUBLECLICKED pause
  exit /B 1
)

rem if configuration files exist, prepend their contents to the script arguments so it can be processed by this runner
call :parse_config "%SCRIPT_CONF_FILE%" SCRIPT_CONF_ARGS

call :process_args %SCRIPT_CONF_ARGS% %%*

set _JAVA_OPTS=!_JAVA_OPTS! !_JAVA_PARAMS!

if defined CUSTOM_MAIN_CLASS (
    set MAIN_CLASS=!CUSTOM_MAIN_CLASS!
) else (
    set MAIN_CLASS=!APP_MAIN_CLASS!
)

rem Call the application and pass all arguments unchanged.
"%_JAVACMD%" !_JAVA_OPTS! !DELTA_SHARING_SERVER_OPTS! -cp "%APP_CLASSPATH%" %MAIN_CLASS% !_APP_ARGS!

@endlocal

exit /B %ERRORLEVEL%


rem Loads a configuration file full of default command line options for this script.
rem First argument is the path to the config file.
rem Second argument is the name of the environment variable to write to.
:parse_config
  set _PARSE_FILE=%~1
  set _PARSE_OUT=
  if exist "%_PARSE_FILE%" (
    FOR /F "tokens=* eol=# usebackq delims=" %%i IN ("%_PARSE_FILE%") DO (
      set _PARSE_OUT=!_PARSE_OUT! %%i
    )
  )
  set %2=!_PARSE_OUT!
exit /B 0


:add_java
  set _JAVA_PARAMS=!_JAVA_PARAMS! %*
exit /B 0


:add_app
  set _APP_ARGS=!_APP_ARGS! %*
exit /B 0


rem Processes incoming arguments and places them in appropriate global variables
:process_args
  :param_loop
  call set _PARAM1=%%1
  set "_TEST_PARAM=%~1"

  if ["!_PARAM1!"]==[""] goto param_afterloop


  rem ignore arguments that do not start with '-'
  if "%_TEST_PARAM:~0,1%"=="-" goto param_java_check
  set _APP_ARGS=!_APP_ARGS! !_PARAM1!
  shift
  goto param_loop

  :param_java_check
  if "!_TEST_PARAM:~0,2!"=="-J" (
    rem strip -J prefix
    set _JAVA_PARAMS=!_JAVA_PARAMS! !_TEST_PARAM:~2!
    shift
    goto param_loop
  )

  if "!_TEST_PARAM:~0,2!"=="-D" (
    rem test if this was double-quoted property "-Dprop=42"
    for /F "delims== tokens=1,*" %%G in ("!_TEST_PARAM!") DO (
      if not ["%%H"] == [""] (
        set _JAVA_PARAMS=!_JAVA_PARAMS! !_PARAM1!
      ) else if [%2] neq [] (
        rem it was a normal property: -Dprop=42 or -Drop="42"
        call set _PARAM1=%%1=%%2
        set _JAVA_PARAMS=!_JAVA_PARAMS! !_PARAM1!
        shift
      )
    )
  ) else (
    if "!_TEST_PARAM!"=="-main" (
      call set CUSTOM_MAIN_CLASS=%%2
      shift
    ) else (
      set _APP_ARGS=!_APP_ARGS! !_PARAM1!
    )
  )
  shift
  goto param_loop
  :param_afterloop

exit /B 0

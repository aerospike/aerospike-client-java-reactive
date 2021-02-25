Aerospike Reactor Java Client Library
=====================================

This project contains the files necessary to build the Java [Reactor](https://projectreactor.io/) client library 
interface to Aerospike database servers. 

AerospikeReactorClient now supports reactive methods. 

The Netty library artifacts (netty-transport and netty-handler) are declared optional.
If your application's build file (pom.xml) declares these Netty library artifacts as 
dependencies, then the Netty libraries will be included in your application's jar.
Otherwise, you application's jar will not include any Netty code.

The source code can be imported into your IDE and/or built using Maven.

    mvn install 
    
Tests are disabled by default. To run tests you need an environment with aerospike running
The simplest way is to install docker and run:

    docker run -tid --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server

Test Usage:

    ./run_tests <options>

    options:
    -h,--host <arg>       Server hostname (default: localhost)
    -U,--user <arg>       User name. Use for servers that require authentication.
    -P,--password <arg>   Password. Use for servers that require authentication.
    -n,--namespace <arg>  Namespace (default: test)
    -p,--port <arg>       Server port (default: 3000)
    -s,--set <arg>        Set name. Use 'empty' for empty set (default: test)
    -tls,--tlsEnable      Use TLS/SSL sockets
    -tlsCiphers,--tlsCipherSuite <arg>
                          Allow TLS cipher suites
                          Values:  cipher names defined by JVM separated by comma
                          Default: null (default cipher list provided by JVM)
    -tp,--tlsProtocols <arg>
                          Allow TLS protocols
                          Values:  SSLv3,TLSv1,TLSv1.1,TLSv1.2 separated by comma
                          Default: TLSv1.2
    -tr,--tlsRevoke <arg> 
                          Revoke certificates identified by their serial number
                          Values:  serial numbers separated by comma
                          Default: null (Do not revoke certificates)
    -d,--debug            Run in debug mode.
    -u,--usage            Print usage.

Test Examples:

    ./run_tests
    ./run_tests -h host
    ./run_tests -h host -p 3000 -n myns -s myset

Test TLS Examples:

    ./run_tests -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -h hostname:tlsname:tlsport -tls
    
    ./run_tests -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -h hostname:tlsname:tlsport -tls -netty

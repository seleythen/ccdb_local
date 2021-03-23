#!/bin/bash

cd `dirname $0`

# Tomcat version to embed in this project
VER="9.0.44"

T="apache-tomcat-$VER"

if [ ! -d "$T" ]; then
    echo "Downloading Tomcat $VER"

    curl -L "https://archive.apache.org/dist/tomcat/tomcat-9/v$VER/bin/apache-tomcat-$VER.tar.gz" -o apache-tomcat.tar.gz || exit 1

    tar -xf apache-tomcat.tar.gz || exit 2
    rm apache-tomcat.tar.gz

    rm -rf apache-tomcat-$VER/{conf,logs,temp,webapps,work,waffle,LICENSE,NOTICE,RELEASE-NOTES,RUNNING.txt} apache-tomcat-$VER/bin/{*.sh,*.bat,*.tar.gz,*.xml}
fi

rm -f apache-tomcat
ln -s "$T" apache-tomcat

if [ ! -f ../lib/lazyj.jar ]; then
    echo "Downloading lazyj"
    curl -L "http://lazyj.sf.net/download/lazyj.jar" -o ../lib/lazyj.jar
fi

if [ ! -f ../lib/postgresql.jar ]; then
    echo "Downloading PostgreSQL JDBC driver"
    curl -L "https://jdbc.postgresql.org/download/postgresql-42.2.19.jar" -o ../lib/postgresql.jar
fi

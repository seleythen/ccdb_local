# CCDB (Calibration and Conditions Database) repository for ALICE

## Description
This project implements the server side of CCDB and QC.

Currently there are two implementations that can be used:
* The reference implementation, PostgreSQL-backed
* A local, filesystem only, version for local development, debugging, embedding etc

All versions implement the same REST [API](API.md). Clients can rely on the relative paths described in the API but should be configurable in terms of the base URL to be used.

## Compiling
The project requires a JDK on the local machine to compile. Any version after 8 is ok for as long as it can be found in *PATH*. In *package/* you can find a few scripts that do the job, in particular
* package.sh - compiles and also packages the project + all the dependencies in separate applications, **local.jar** and **sql.jar**
* compile.sh - the compilation step alone

Most of the dependencies (JAliEn, ApMon, MonALISA, LazyJ, BouncyCastle) are already in the *lib/* folder. Tomcat and the PostgreSQL driver are however downloaded first time they are needed using **download-tomcat.sh** from the same folder. Adjust this script in case you want to use a different Tomcat version.

## Running

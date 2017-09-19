This tool will find new and modified artifacts from caom2 and download them to local artifact storage.

Running of this tool requires an implementation of the ArtifactStore.java interface.  This project will build a jar file that has all the infrastructure needed to run this tool, including the Main.java entry point.  To run this tool, create a new gradle project that has your implmentation of ArtifactStore in the classpath.  Below is a template of the build.gradle file that could be used to accomplish this.

    plugins {
        id 'java'
        id 'maven'
        id 'maven-publish'
        id 'com.jfrog.bintray' version '1.7.1'
        id 'application'
    }

    repositories {
        jcenter()
        mavenLocal()
    }

    sourceCompatibility = 1.7

    group = 'org.opencadc'

    version = '0.1'

    mainClassName = 'ca.nrc.cadc.caom2.artifactsync.Main'

    dependencies {
        compile 'org.opoecadc:caom2-artifact-sync[2.3.0,)'
    }

# This project and its code and functionality is not representative of MarkLogic Server and is not supported by MarkLogic.


# marklogic-rdf4j v1.2.2

## Introduction

The markLogic-rdf4j API is a [RDF4J](http://rdf4j.org/) Repository
implementation exposing [MarkLogic](http://www.marklogic.com) semantic
[features](http://www.marklogic.com/what-is-marklogic/features/semantics/).

* Transactions: Fully compliant ACID transactions.
* Variable bindings: Set a binding(s) name, language tag, and value.
* Inference (ruleset configuration): Enable inference rulesets .
* Combination of SPARQL with MarkLogic document query: Constrain SPARQL query with MarkLogic queries.
* Optimized pagination of SPARQL result sets: Efficient paging of results.
* Permissions: Manage permissions on triples.

## Before you start

The markLogic-rd4j API supports [RDF4J v2.4.3](http://rdf4j.org/).

### Setup MarkLogic

Ensure MarkLogic 9.0-8 or greater is installed and running. To use
marklogic-rdf4j applications you will need access to a running MarkLogic
server.

## Usage

### Quick Start

The markLogic-rdf4j API is available via [Maven
Central](http://central.maven.org/maven2/artifact/com.marklogic/marklogic-rdf4j/1.2.2).

For gradle projects, include the following dependency in your `build.gradle`:

```
dependencies {
    compile group: 'com.marklogic', name: 'marklogic-rdf4j', version: '1.2.2'
}
```

For maven projects, include in your pom.xml:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-rdf4j</artifactId>
    <version>1.2.2</version>
</dependency>
```

## Build and Use from develop branch

This section describes how to build and test marklogic-rdf4j API from _develop_ branch.

#### Setup MarkLogic Java API Client (optional)

marklogic-rdf4j depends on 
[MarkLogic Java API Client v4.1.2](http://mvnrepository.com/artifact/com.marklogic/marklogic-client-api/4.1.2)
and should pull down this version from maven central.

To optionally build marklogic-rdf4j with _develop_ branch version of MarkLogic Java API Client:

1. Clone or download [MarkLogic Java API client](https://github.com/marklogic/java-client-api/tree/develop) _develop_ branch.
2. Build and deploy Java API client to local maven repo.

```
mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
```

Verify that Java API client has been deployed to your local maven repo.

3. Edit marklogic-RDF4J/build.gradle to use that build

```
    compile('com.marklogic:marklogic-client-api:4.1.2') 

```


#### Setup and Test markLogic-rdf4j API

marklogic-rdf4j depends on MarkLogic v9.0-8 or greater installed and running;

1. Clone or download [marklogic-rdf4j](https://github.com/marklogic/marklogic-rdf4j/tree/master) _master_ branch.
2. Review [marklogic-rdf4j/gradle.properties](marklogic-rdf4j/gradle.properties) which defines test deployment settings.
3. Run gradle target that provisions MarkLogic with everything required (database,REST server,etc.).

```
gradle marklogic-rdf4j:mlDeploy
```
You should be able to test marklogic-rdf4j repository by running:
```
gradle marklogic-rdf4j:test
```

#### Build and Deploy

Build and deploy a local maven marklogic-rdf4j build by running;

```
gradle marklogic-rdf4j:install

```

optionally you can build the jar without running tests.

```
gradle build -x test
```

and copy resultant marklogic-rdf4j/build/libs/marklogic-rdf4j-1.2.2.jar.

### Examples

The [marklogic-rdf4j-examples](marklogic-rdf4j-examples) folder contains
a sample project that demonstrates usage of marklogic-rdf4j.

### Javadocs

Latest 
[javadocs are here](http://marklogic.github.io/marklogic-rdf4j/marklogic-rdf4j/build/docs/javadoc/index.html)

You may generate javadocs by running;

```
gradle marklogic-rdf4j:javadoc

```

### License

[Apache License v2.0](LICENSE)



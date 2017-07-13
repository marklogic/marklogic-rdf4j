package com.marklogic.semantics.rdf4j.benchmarks;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import org.openjdk.jmh.annotations.Benchmark;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MarkLogicRepositoryConnectionNaivePerfTest {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConnectionNaivePerfTest.class);

    @Benchmark
    public void perfNaiveQuery1()
            throws Exception {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("mlUsername");
        String pass = props.getProperty("mlPassword");
        // extrude to semantics.utils

        MarkLogicRepository rep = new MarkLogicRepository(host,port,user,pass,"DIGEST");
        rep.initialize();

        MarkLogicRepositoryConnection conn = rep.getConnection();
        rep.shutDown();
        rep.initialize();

        File inputFile = new File("src/jmh/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createIRI("http://marklogic.com/test/context3");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3);

        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");

        conn.begin();
        int count = 0;
        for (int i=0 ; i<10000 ; i++){
            Literal obj = f.createLiteral("Alice" + count);
            if ( (i & 1) == 0 ) {
                Statement st = f.createStatement(alice, name,obj);
                conn.add(st,context1,context2);
            }else{
                Statement st = f.createStatement(alice, name,obj);
                conn.add(st);
            }
            count = count + 1;
        }
        conn.commit();

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        while(results.hasNext()) {
            BindingSet bindingSet = results.next();
            @SuppressWarnings("unused")
            Value sV = bindingSet.getValue("s");
            @SuppressWarnings("unused")
            Value pV = bindingSet.getValue("p");
            @SuppressWarnings("unused")
            Value oV = bindingSet.getValue("o");
        }
        Resource subject = conn.getValueFactory().createIRI("urn:x-local:graph1");
        RepositoryResult<Statement> statements = conn.getStatements(subject, null, null, true, context1);

        conn.clear(context1);
        conn.clear();
        conn.close();
        rep.shutDown();
    }
}

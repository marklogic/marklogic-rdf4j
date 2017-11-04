/*
 * Copyright 2015-2017 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.semantics.rdf4j.examples;

import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.query.MarkLogicTupleQuery;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Example1_Simple {

    protected static Logger logger =LoggerFactory.getLogger(Example1_Simple.class);

    public static void main(String... args) throws RepositoryException, IOException, RDFParseException, MalformedQueryException, QueryEvaluationException {

        // instantiate repository
        MarkLogicRepository repo = new MarkLogicRepository("localhost",8200, new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
        repo.initialize();

        // get repository connection
        MarkLogicRepositoryConnection conn = repo.getConnection();

        // return number of triples contained in repository
        logger.info("number of triples: {}", conn.size());

        // add triples from a file
        File inputFile = new File("src/main/resources/testdata/test-small.owl");
        conn.add(inputFile, null, RDFFormat.RDFXML, (Resource) null);

        logger.info("number of triples: {}", conn.size());

        // add a few constructed triples
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/examples/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/examples/context2");
        ValueFactory f= conn.getValueFactory();
        String namespace = "http://example.org/";
        IRI john = f.createIRI(namespace, "john");
        conn.add(john, RDF.TYPE, FOAF.PERSON,context1);
        conn.add(john, RDFS.LABEL, f.createLiteral("John", XMLSchema.STRING),context2);

        // check if triples with subject john exist in repository
        String checkJohnQuery = "ASK { <http://example.org/john> ?p ?o .}";
        BooleanQuery booleanJohnQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkJohnQuery);
        logger.info("result of query: {}",booleanJohnQuery.evaluate());

        // perform SPARQL query with pagination
        String queryString = "select * { ?s ?p ?o }";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleQuery.setIncludeInferred(true);
        TupleQueryResult results = tupleQuery.evaluate(1,10);

        //iterate through query results
        while(results.hasNext()){
            BindingSet bindings = results.next();
            logger.info("subject:{}",bindings.getValue("s"));
            logger.info("predicate:{}", bindings.getValue("p"));
            logger.info("object:{}", bindings.getValue("o"));
        }

        // clear all triples
        conn.clear();
        logger.info("number of triples: {}", conn.size());

        // close connection and shutdown repository
        conn.close();
        repo.shutDown();

        System.exit(0);
    }
}


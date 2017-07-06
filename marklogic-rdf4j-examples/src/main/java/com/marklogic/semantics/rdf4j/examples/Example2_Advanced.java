/*
 * Copyright 2015-2016 MarkLogic Corporation
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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.query.MarkLogicTupleQuery;
import com.marklogic.semantics.sesame.query.MarkLogicUpdateQuery;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Example2_Advanced {

    protected static Logger logger =LoggerFactory.getLogger(Example2_Advanced.class);

    public static void main(String... args) throws RepositoryException, IOException, RDFParseException, MalformedQueryException, QueryEvaluationException {

        // instantiate MarkLogicRepository with Java api client DatabaseClient
        DatabaseClient adminClient = DatabaseClientFactory.newClient("localhost", 8200, "admin","admin", DatabaseClientFactory.Authentication.DIGEST);
        GraphManager gmgr = adminClient.newGraphManager();
        QueryManager qmgr = adminClient.newQueryManager();

        // create repo and init
        MarkLogicRepository repo = new MarkLogicRepository(adminClient);
        repo.initialize();

        // get repository connection
        MarkLogicRepositoryConnection conn = repo.getConnection();

        // set default rulesets
        conn.setDefaultRulesets(SPARQLRuleset.ALL_VALUES_FROM);

        // set default perms
        conn.setDefaultGraphPerms(gmgr.permission("admin", Capability.READ).permission("admin", Capability.EXECUTE));

        // set a default Constraining Query
        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
        conn.setDefaultConstrainingQueryDefinition(stringDef);

        // return number of triples contained in repository
        logger.info("1. number of triples: {}", conn.size());

        // add a few constructed triples
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/examples/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/examples/context2");
        ValueFactory f= conn.getValueFactory();
        String namespace = "http://example.org/";
        URI john = f.createURI(namespace, "john");

        //use transactions to add triple statements
        conn.begin();
        conn.add(john, RDF.TYPE, FOAF.PERSON, context1);
        conn.add(john, RDFS.LABEL, f.createLiteral("John", XMLSchema.STRING), context2);
        conn.commit();

        logger.info("2. number of triples: {}", conn.size());

        // perform SPARQL query
        String queryString = "select * { ?s ?p ?o }";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        // enable rulesets set on MarkLogic database
        tupleQuery.setIncludeInferred(true);

        // set base uri for resolving relative uris
        tupleQuery.setBaseURI("http://www.example.org/base/");

        // set rulesets for infererencing
        tupleQuery.setRulesets(SPARQLRuleset.ALL_VALUES_FROM, SPARQLRuleset.HAS_VALUE);

        // set a combined query
        String combinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"*\"}}";
        RawCombinedQueryDefinition rawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        tupleQuery.setConstrainingQueryDefinition(rawCombined);

        // evaluate query with pagination
        TupleQueryResult results = tupleQuery.evaluate(1,10);

        //iterate through query results
        while(results.hasNext()){
            BindingSet bindings = results.next();
            logger.info("subject:{}",bindings.getValue("s"));
            logger.info("predicate:{}", bindings.getValue("p"));
            logger.info("object:{}", bindings.getValue("o"));
        }
        logger.info("3. number of triples: {}", conn.size());

        //update query
        String updatequery = "INSERT DATA { GRAPH <http://marklogic.com/test/context10> {  <http://marklogic.com/test/subject> <pp1> <oo1> } }";
        MarkLogicUpdateQuery updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, updatequery,"http://marklogic.com/test/baseuri");

        // set perms to be applied to data
        updateQuery.setGraphPerms(gmgr.permission("admin", Capability.READ).permission("admin", Capability.EXECUTE));

        try {
            updateQuery.execute();
        } catch (UpdateExecutionException e) {
            e.printStackTrace();
        }

        logger.info("4. number of triples: {}", conn.size());

        // clear all triples
        conn.clear();
        logger.info("5. number of triples: {}", conn.size());

        // close connection and shutdown repository
        conn.close();
        repo.shutDown();
    }
}
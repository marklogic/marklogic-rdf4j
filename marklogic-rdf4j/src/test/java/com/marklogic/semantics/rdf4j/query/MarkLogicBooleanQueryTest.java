/*
 * Copyright 2015-2018 MarkLogic Corporation
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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.Rdf4jTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * test BooleanQuery
 *
 *
 */
public class MarkLogicBooleanQueryTest extends Rdf4jTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp() throws RepositoryException, FileNotFoundException {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");
        File testData = new File("src/test/resources/testdata/test.owl");

        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);

        gmgr.write("http://example.org/test/g27", new FileHandle(testData));
    }

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        conn.close();
        conn = null;
        rep.shutDown();
        rep = null;
        logger.info("tearDown complete.");
        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.delete("http://example.org/test/g27");
    }

    @Test
    public void testBooleanQuery()
            throws Exception {
        String queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

    @Test
    public void testBooleanQueryWithOptimizeLevel()
            throws Exception {
        String queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        conn.setOptimizeLevel(0);
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
        conn.setOptimizeLevel(-1);
    }

    @Test
    public void testBooleanQueryWithOverloadedMethods()
            throws Exception {
        String queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(queryString);
        booleanQuery = conn.prepareBooleanQuery(queryString,"http://marklogic.com/test/baseuri");
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

    @Test(expected=org.eclipse.rdf4j.query.QueryEvaluationException.class)
    public void testBooleanQueryQueryEvaluationException()
            throws Exception {
        String queryString = "ASK GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
    }

    @Test(expected=org.eclipse.rdf4j.query.QueryEvaluationException.class)
    public void testBooleanQueryMalformedException()
            throws Exception {
        String queryString = "ASK1 GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
    }

}

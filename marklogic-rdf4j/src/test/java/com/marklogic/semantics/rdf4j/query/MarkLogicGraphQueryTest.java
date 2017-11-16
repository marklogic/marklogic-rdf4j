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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.Rdf4jTestBase;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * test GraphQuery
 *
 *
 */
public class MarkLogicGraphQueryTest extends Rdf4jTestBase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
        File testData = new File(TESTFILE_OWL);

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
    public void testConstructQuery()
            throws Exception {
        String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "\n" +
                "construct { ?s  test:test \"0\"} WHERE  {?s nn:childOf nn:Eve . }";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
        results.close();
    }

    @Test
    public void testGraphQueryWithBaseURIInline()
            throws Exception {
        String queryString ="BASE <http://marklogic.com/test/baseuri>\n" +
                "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "construct { ?s  test:test <relative>} WHERE {?s nn:childOf nn:Eve . }";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
        results.close();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/45
    @Test
    public void testGraphQueryWithBaseURI()
            throws Exception {
        String queryString =
                "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                        "PREFIX test: <http://marklogic.com#test>\n" +
                        "construct { ?s  test:test <relative>} WHERE {?s nn:childOf nn:Eve . }";
        GraphQuery graphQuery = conn.prepareGraphQuery(queryString, "http://marklogic.com/test/baseuri/");
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://marklogic.com/test/baseuri/relative", st1.getObject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://marklogic.com/test/baseuri/relative", st2.getObject().stringValue());
        results.close();
    }

    @Ignore
    @Test
    public void testGraphQueryWithBaseURIWithEmptyBaseURI()
            throws Exception {
        String queryString =
                "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                        "PREFIX test: <http://marklogic.com#test>\n" +
                        "construct { ?s  test:test <relative>} WHERE {?s nn:childOf nn:Eve . }";
        GraphQuery graphQuery = conn.prepareGraphQuery(queryString, "");
        exception.expect(QueryEvaluationException.class);

        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://relative", st1.getObject().stringValue());
        @SuppressWarnings("unused")
        Statement st2 = results.next();
        Assert.assertEquals("http://relative", st1.getObject().stringValue());
        results.close();
    }

    @Ignore
    public void testConstructQueryWithWriter()
            throws Exception {
        RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, System.out);

        @SuppressWarnings("unused")
        String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "\n" +
                "construct { ?s  test:test \"0\"} WHERE {?s nn:childOf nn:Eve . }";
        conn.prepareGraphQuery(QueryLanguage.SPARQL,
                "CONSTRUCT {?s ?p ?o } WHERE {?s ?p ?o } ").evaluate(writer);
    }


    @Test
    public void testDescribeQuery()
            throws Exception {
        String queryString = "DESCRIBE <http://semanticbible.org/ns/2006/NTNames#Shelah>";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Shelah", st1.getSubject().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#childOf", st1.getPredicate().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#CainanSonOfArphaxad", st1.getObject().stringValue());
        results.close();
    }

    @Test
    public void testPrepareGraphQueryWithSingleResult() throws Exception
    {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1,context1);

        String query = " DESCRIBE <http://example.org/people/alice> ";
        GraphQuery queryObj = conn.prepareGraphQuery(query);
        GraphQueryResult result = queryObj.evaluate();

        Assert.assertTrue(result != null);
        Assert.assertTrue(result.hasNext());
        @SuppressWarnings("unused")
        Statement st = result.next();
        Assert.assertFalse(result.hasNext());
        result.close();
        conn.clear(context1);
    }

    @Test
    public void testPrepareGraphQueryWithNoResult() throws Exception
    {
        String query = "DESCRIBE <http://example.org/nonexistant>";
        GraphQuery queryObj = conn.prepareGraphQuery(query);
        GraphQueryResult result = queryObj.evaluate();

        Assert.assertTrue(result != null);
        Assert.assertFalse(result.hasNext());
        result.close();
    }

    @Ignore
    public void testCreateGraph() throws Exception
    {
        conn.prepareUpdate("CREATE GRAPH <http://example1.org>").execute();
    }

    // result.close() throws an NPE
    // https://github.com/marklogic/marklogic-sesame/issues/257
    @Test
    public void testPrepareGraphQueryClose() throws Exception
    {
        String query = "DESCRIBE <http://example.org/ontology/name>";
        GraphQuery queryObj = conn.prepareGraphQuery(query);
        GraphQueryResult result = queryObj.evaluate();
        result.close();
    }

}

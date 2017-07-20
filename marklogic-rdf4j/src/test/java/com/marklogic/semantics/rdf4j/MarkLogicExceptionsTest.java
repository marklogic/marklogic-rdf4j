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
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * RDF4J API.
 */
package com.marklogic.semantics.rdf4j;

import com.marklogic.client.ResourceNotFoundException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

import static junit.framework.TestCase.fail;

/**
 * tests that the correct exceptions are thrown
 *
 * @author James Fuller
 */
public class MarkLogicExceptionsTest extends Rdf4jTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn =rep.getConnection();
        logger.info("test setup complete.");
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
    }

    @Test
    public void TestMalformedQuery() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        exception.expect(QueryEvaluationException.class);
        @SuppressWarnings("unused")
        TupleQueryResult results = conn.prepareTupleQuery("A malformed query").evaluate();
        results.close();
    }

    @Test
    public void TestMalformedBooleanQuery() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        exception.expect(QueryEvaluationException.class);
        @SuppressWarnings("unused")
        boolean results = conn.prepareBooleanQuery("A malformed query").evaluate();
    }

    @Test
    public void TestMalformedUpdateQUery() throws RepositoryException, MalformedQueryException, UpdateExecutionException {
        exception.expect(UpdateExecutionException.class);
        conn.prepareUpdate("A malformed query").execute();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/65
    @Test
    public void testAddMalformedTurtle() throws Exception {
        File inputFile = new File("src/test/resources/testdata/malformed-literals.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        exception.expect(RDFParseException.class);
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.clear(context1);
    }
    @Test
    public void testAddMalformedWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/malformed-literals.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        exception.expect(RDFParseException.class);
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context3");
        conn.add(is, baseURI, RDFFormat.TURTLE, context1);
        conn.clear(context1);
    }

    @Test
    public void testIncorrectIsolatedLevel() throws Exception {
        exception.expect(IllegalStateException.class);
        conn.setIsolationLevel(IsolationLevels.READ_UNCOMMITTED);
    }

    @Test
    public void updateWithWrongPerms() throws RepositoryException, MalformedQueryException, UpdateExecutionException {
        readerRep.initialize();
        MarkLogicRepositoryConnection testReaderCon = readerRep.getConnection();
        exception.expect(UpdateExecutionException.class);
        testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();
    }

    @Test
    public void testAddWithInputStream() throws Exception {
        exception.expect(ResourceNotFoundException.class);

        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createIRI("http://marklogic.com/test/context3");
        Resource context4 = conn.getValueFactory().createIRI("http://marklogic.com/test/context4");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3); // TBD - add multiple context
        conn.clear(context3, context4); // ensure we throw error as context not defined
    }


    @Test
    public void testTransaction3() throws Exception {
        exception.expect(ResourceNotFoundException.class);
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/my-graph");
        conn.begin();
        conn.clear(context1);
        conn.rollback();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/121
    @Test
    public void testDanglingRollback() throws Exception {
        exception.expect(MarkLogicTransactionException.class);
        conn.rollback();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/174
    @Test
    // Note- we have relaxed the conditions for throwing this exception
    public void testEmptyCommit() throws Exception {
        exception.expect(MarkLogicTransactionException.class);
        logger.info("active:{}",conn.isActive());
        conn.commit();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/110
    @Test
    public void testAddingIfClosed() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        exception.expect(RepositoryException.class);
        conn.close();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
    }

    @Test
    public void testException1() throws Exception {
        exception.expect(MarkLogicRdf4jException.class);
        throw new MarkLogicRdf4jException("testing exception");
    }

    @Test
    public void testException2() throws Exception {
            exception.expect(MarkLogicRdf4jException.class);
            throw new MarkLogicRdf4jException("testing exception",null);
    }

    @Test
    public void testTransactionException1() throws Exception {
        exception.expect(MarkLogicTransactionException.class);
        throw new MarkLogicTransactionException("testing exception");
    }

    @Test
    public void testTransactionException2() throws Exception {
        exception.expect(MarkLogicTransactionException.class);
        throw new MarkLogicTransactionException("testing exception",null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/319
    @Test
    public void testBeginTransactionForRestReader(){
        readerRep.initialize();
        MarkLogicRepositoryConnection con = readerRep.getConnection();
        ValueFactory vf = con.getValueFactory();
        IRI gary = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        IRI gender = vf.createIRI("http://marklogicsparql.com/addressbook#gender");
        Statement st = vf.createStatement(gary, age, gender);

        exception.expect(RepositoryException.class);
        con.begin(); //causes exception
        con.add(st, vf.createIRI("http://abc"));
        con.commit();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/320
    @Test
    public void testCloseConnectionForRestReader(){
        readerRep.initialize();

        MarkLogicRepositoryConnection testReaderCon = readerRep.getConnection();
        ValueFactory vf = testReaderCon.getValueFactory();
        IRI gary = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        IRI gender = vf.createIRI("http://marklogicsparql.com/addressbook#gender");

        try{
            testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();
            Assert.assertTrue(false);
        }
        catch(Exception e){
            Assert.assertTrue(e instanceof UpdateExecutionException);
        }


        final Statement st1 = vf.createStatement(gary, age, gender);

        try{
            testReaderCon.add(st1, vf.createIRI("http://abc"));
        }
        catch(Exception e){
            Assert.assertTrue(e instanceof UpdateExecutionException);
        }

        exception.expect(RepositoryException.class);
        testReaderCon.close();
    }
}

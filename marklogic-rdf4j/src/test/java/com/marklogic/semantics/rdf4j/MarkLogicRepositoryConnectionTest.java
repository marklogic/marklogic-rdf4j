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

import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryFactory;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * tests add, remove, hasSatement, getStatements, export, etc
 *
 * @author James Fuller
 */
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MarkLogicRepositoryConnectionTest extends Rdf4jTestBase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

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
        if( conn.isOpen() && conn.isActive()){conn.rollback();}
        //conn.clear();
        if(conn.isOpen()){conn.clear();}
        conn.close();
        rep.shutDown();
        conn=null;
        logger.info("tearDown complete.");
    }

    @Test
    public void testMarkLogicRepositoryConnectionOpen()
            throws Exception {
        Assert.assertEquals(true, conn.isOpen());
    }

    @Test
    public void testMarkLogicRepositoryConnection()
            throws Exception {

        Assert.assertNotNull("Expected repository to exist.", rep);
        Assert.assertTrue("Expected repository to be initialized.", rep.isInitialized());
        rep.shutDown();
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("validUsername");
        String pass = props.getProperty("validPassword");

        rep = new MarkLogicRepository(host, port, user, pass, "DIGEST");

        Assert.assertNotNull("Expected repository to exist.", rep);
        Assert.assertFalse("Expected repository to not be initialized.", rep.isInitialized());
        rep.initialize();
        conn = rep.getConnection();
        Assert.assertTrue("Expected repository to be initialized.", rep.isInitialized());
        rep.shutDown();
        Assert.assertFalse("Expected repository to not be initialized.", rep.isInitialized());
        rep.initialize();
        conn = rep.getConnection();
        Assert.assertNotNull("Expected repository to exist.", rep);
    }

    @Test
    public void testClearAll()
            throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <pp1> <oo1> } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        conn.clear();
        Assert.assertEquals(0, conn.size());
    }

    @Test
    public void testClearSome()
            throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        File inputFile3 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile3, "http://example.org/example1/", RDFFormat.TURTLE, context2);
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <http://marklogic.com/test/pp1> \"oo1\" } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        conn.clear(null, context1);
        Assert.assertEquals(5, conn.size());
        conn.clear();
    }

    @Test
    public void testGetStatement1()
            throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        File inputFile3 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile3, "http://example.org/example1/", RDFFormat.TURTLE, context2);

        conn.clear(null, context1);
        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true);
        Model model = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(4, model.size());
        conn.clear();
    }

    @Test
    public void testClearWithContext()
            throws Exception {
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/cleartest> <pp1> <oo1> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);

        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/test/ns/cleartest"));
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
    }

    @Test
    public void testAddTurtle() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1, context2);
        conn.clear(context1, context2);
    }

    // TBD- when base uri is wired into java api client, enable this test
    @Ignore
    public void testAddTurtleUseURLForBaseURI() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-3.ttl");
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");
        conn.add(inputFile, null, RDFFormat.TURTLE, context1, context2);
        conn.clear(context1, context2);
    }
    // https://github.com/marklogic/marklogic-sesame/issues/70
    @Test
    public void testAddTurtleWithNullContext() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);
        Assert.assertEquals(4, conn.size((Resource) null));
        Assert.assertEquals(4, conn.size());
        conn.clear((Resource) null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/64
    // not implemented just yet
    @Ignore
    public void testAddGZippedRDF() throws Exception {
        File inputFile = new File("src/test/resources/testdata/databases.rdf.gz");
        FileInputStream fis = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");
        conn.add(fis, baseURI, RDFFormat.RDFXML, context1, context2);
        conn.clear(context1, context2);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/19
    @Test
    public void testAddTurtleWithDefaultContext() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile, null, RDFFormat.TURTLE);
        String checkQuery = "PREFIX dc:<http://purl.org/dc/elements/1.1/> PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> ASK { <urn:x-local:graph1> dc:publisher ?o .}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        conn.setNamespace("dc", "http://purl.org/dc/elements/1.1/");
        Assert.assertTrue(booleanQuery.evaluate());
        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/semantics#default-graph"));
    }

    @Test
    public void testAddWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createIRI("http://marklogic.com/test/context3");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3); // TBD - add multiple context
        conn.clear(context3);
    }

    // this test requires access to https://raw.githubusercontent.com/marklogic/marklogic-rdf4j/develop/marklogic-rdf4j/src/test/resources/testdata/testData.trig
    @Test
    public void testAddTrigWithURL() throws Exception {
        URL dataURL = new URL("https://raw.githubusercontent.com/marklogic/marklogic-rdf4j/develop/marklogic-rdf4j/src/test/resources/testdata/testData.trig?token=AApzyAXWDMZiXGGf9DFnhq534MpEP-tKks5VwxFswA%3D%3D");

        Resource context1 = conn.getValueFactory().createIRI("http://example.org/g1");
        Resource context2 = conn.getValueFactory().createIRI("http://example.org/g2");
        Resource context3 = conn.getValueFactory().createIRI("http://example.org/g3");
        Resource context4 = conn.getValueFactory().createIRI("http://example.org/g4");
        Resource context5 = conn.getValueFactory().createIRI("http://example.org/o1");
        Resource context6 = conn.getValueFactory().createIRI("http://example.org/o2");
        Resource context7 = conn.getValueFactory().createIRI("http://example.org/o3");
        Resource context8 = conn.getValueFactory().createIRI("http://example.org/o4");
        conn.add(dataURL, dataURL.toString(), RDFFormat.TRIG, context3, context4);

        String checkQuery = "ASK { <http://example.org/r1> <http://example.org/p1> \"string value 0\" .}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        Assert.assertTrue(booleanQuery.evaluate());

        conn.clear(context1,context2,context3,context4,context5,context6,context7,context8);
    }

    @Test
    public void testAddNQuads() throws Exception{
        File inputFile = new File("src/test/resources/testdata/nquads1.nq");
        String baseURI = "http://example.org/example1/";

        Resource graph1 = conn.getValueFactory().createIRI("http://example.org/graph1");
        Resource graph2 = conn.getValueFactory().createIRI("http://example.org/graph2");
        Resource graph3 = conn.getValueFactory().createIRI("http://example.org/graph3");
        Resource graph4 = conn.getValueFactory().createIRI("http://example.org/graph4");

        conn.add(inputFile,baseURI,RDFFormat.NQUADS);

        String checkQuery = "ASK {GRAPH <http://example.org/graph4> { <http://example.org/kennedy/person1> <http://example.org/kennedy/death-year> '1969' . } }";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        Assert.assertTrue(booleanQuery.evaluate());

        conn.clear(graph1,graph2,graph3,graph4);
    }

    @Test
    public void testAddNquadWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/nquads1.nq");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource graph1 = conn.getValueFactory().createIRI("http://example.org/graph1");
        Resource graph2 = conn.getValueFactory().createIRI("http://example.org/graph2");
        Resource graph3 = conn.getValueFactory().createIRI("http://example.org/graph3");
        Resource graph4 = conn.getValueFactory().createIRI("http://example.org/graph4");
        conn.add(is, baseURI, RDFFormat.NQUADS);
        conn.clear(graph1, graph2, graph3, graph4);
    }

    @Test
    public void testAddRemoveStatementWithMultipleContext() throws Exception {

        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context7");
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context8");
        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        String checkAliceQuery = "ASK { <http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.remove(alice, RDF.TYPE, person, context5);
        conn.remove(alice, name, alicesName, context5, context6);
        conn.remove(bob, RDF.TYPE, person, context5);
        conn.remove(bob, name, bobsName, context5, context6);

        Assert.assertFalse(booleanAliceQuery.evaluate());

        conn.clear(context5,context6);
    }

    @Test
    public void testContextIDs()
            throws Exception {
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context7");
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context8");
        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        RepositoryResult<Resource> result = conn.getContextIDs();
        try {
            Assert.assertTrue("result should not be empty", result.hasNext());
            Resource result1 = result.next();
            Assert.assertEquals("http://marklogic.com/semantics#default-graph", result1.stringValue());
            result1 = result.next();
            Assert.assertEquals("http://marklogic.com/test/context7", result1.stringValue());
            result1 = result.next();
            Assert.assertEquals("http://marklogic.com/test/context8", result1.stringValue());
        } finally {
            result.close();
        }
        conn.clear(context5, context6);
    }

    @Test
    public void testTransaction1() throws Exception {
        File inputFile = new File("src/test/resources/testdata/named-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/transactiontest");
        conn.begin();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.rollback();
    }

    @Test
    public void testTransaction2() throws Exception {
        File inputFile = new File("src/test/resources/testdata/named-graph-1.ttl");

        String baseURI = "http://example.org/example1/";

        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/transactiontest");
        conn.begin();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.commit();
        conn.clear(context1);
    }

    @Test
    public void testTransaction3() throws Exception {
        File inputFile = new File("src/test/resources/testdata/named-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/transactiontest");
        conn.begin();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.rollback();
    }

    @Test
    public void testOpen() throws Exception {
        Assert.assertEquals(true, conn.isOpen());
        conn.close();
        Assert.assertEquals(false, conn.isOpen());
    }

    @Test
    public void testActive() throws Exception {
        Assert.assertEquals(false, conn.isActive());
        conn.begin();
        Assert.assertEquals(true, conn.isActive());
    }

    @Test
    public void testSizeWithEmptyGraph() throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/nonexistent");
        Assert.assertEquals(0, conn.size(context1));
    }

    @Test
    public void testSizeWithSmallerGraph() throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice");
        conn.add(alice, RDF.TYPE, person, context1);
        conn.add(alice, name, alicesName,context1);
        Assert.assertEquals(2, conn.size(context1));
        conn.clear(context1);
    }

    @Test
    public void testModel() throws Exception{
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context6");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        //TBD- need to be able to set baseURI
       // RepositoryResult<Statement> statements = conn.getStatements(alice, null, null, true,context5);

        //Model aboutAlice = Iterations.addAll(statements, new LinkedHashModel());
conn.sync();
        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context5> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.remove(alice, RDF.TYPE, person, context5);
        conn.remove(alice, name, alicesName, context5, context6);
        conn.remove(bob, RDF.TYPE, person, context5);
        conn.remove(bob, name, bobsName, context5, context6);

        Assert.assertFalse(booleanAliceQuery.evaluate());

        conn.clear(context5,context6);
    }

    @Test
    public void testAddStatements() throws Exception{
        Resource context = conn.getValueFactory().createIRI("http://marklogic.com/test/context");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI age = f.createIRI("http://example.org/ontology/age");
        Literal bobsAge = f.createLiteral(123123123123D);
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, name, alicesName, context);
        conn.add(bob, age, bobsAge, context);

        String checkAliceQuery = "ASK { <http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        String checkBobQuery = "ASK { <http://example.org/people/bob> <http://example.org/ontology/age> '123123123123'^^<http://www.w3.org/2001/XMLSchema#double> .}";
        BooleanQuery booleanBobQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkBobQuery);
        Assert.assertTrue(booleanBobQuery.evaluate());

        conn.clear(context);
    }

    @Test
    public void testModelWithIterator() throws Exception{
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice1");

        conn.add(alice, RDF.TYPE, person, context1);
        conn.add(alice, name, alicesName, context1);

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        RepositoryResult<Statement> statements = conn.getStatements(alice, null, null, true,context1);

        conn.add(statements, context2);
        conn.clear(context1);

        checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context2> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context2);
    }

    @Test
    public void testStatementWithDefinedContext1() throws Exception{
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName, context1);
        conn.add(st1);
        assertEquals("Statement must incrment size of database.", 1, conn.size());
        assertEquals("Statement must incrment size of database.", 1, conn.size(context1));
        assertEquals("Statement must not incrment size of default graph.", 0, conn.size((Resource) null));
        

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context1);
    }

    @Test
    public void testStatementWithDefinedContext2() throws Exception{
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context1);
    }

    @Test
    public void testGetStatements() throws Exception{
        File inputFile = new File(TESTFILE_OWL);
        conn.add(inputFile,null,RDFFormat.RDFXML);
        ValueFactory f= conn.getValueFactory();
        IRI subj = f.createIRI("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true);
        Assert.assertTrue(statements.hasNext());
        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/semantics#default-graph"));
    }

    @Test
    public void testGetStatementsEmpty() throws Exception{
        File inputFile = new File(TESTFILE_OWL);
        conn.add(inputFile, null, RDFFormat.RDFXML);
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/my-graph");
        conn.add(inputFile, null, RDFFormat.RDFXML, context1);

        ValueFactory f= conn.getValueFactory();
        IRI subj = f.createIRI("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata1");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true, context1);

        Assert.assertFalse(statements.hasNext());
        conn.clear(context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/138
    @Test
    public void testHasStatement() throws Exception
    {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        Assert.assertTrue(conn.hasStatement(st1, false, context1));
        Assert.assertTrue(conn.hasStatement(st1, false, context1, null));
        Assert.assertFalse(conn.hasStatement(st1, false, null));
        Assert.assertFalse(conn.hasStatement(st1, false, (Resource) null));
        Assert.assertTrue(conn.hasStatement(st1, false));
        Assert.assertTrue(conn.hasStatement(null, null, null, false));
        conn.clear(context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/153
    @Test
    public void testHasStatement2() throws Exception
    {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        Assert.assertTrue(conn.hasStatement(st1, false));
        Assert.assertFalse(conn.hasStatement(st1, false, (Resource) null));
        Assert.assertFalse(conn.hasStatement(st1, false, null));

        conn.clear(context1);
    }

    @Test
    public void testExportStatements()
            throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        RDFXMLWriter rdfWriter = new RDFXMLWriter(out);

        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<rdf:RDF\n" +
                "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://example.org/people/alice\">\n" +
                "\t<name xmlns=\"http://example.org/ontology/\" rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">Alice</name>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "</rdf:RDF>";

        conn.exportStatements(alice, null, alicesName, true, rdfWriter, context1);
        Assert.assertEquals(expected, out.toString());
        conn.clear(context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/108
    @Test
    public void testExportStatementsAllNull()
            throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        RDFXMLWriter rdfWriter = new RDFXMLWriter(out);

        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<rdf:RDF\n" +
                "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://example.org/people/alice\">\n" +
                "\t<name xmlns=\"http://example.org/ontology/\" rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">Alice</name>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "</rdf:RDF>";

        conn.exportStatements(null, null, null, false, rdfWriter, context1);
        Assert.assertEquals(expected, out.toString());
        conn.clear(context1);
    }

    @Ignore
    public void testIntegrateWithRemoteRepository() throws Exception{
        final Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");

        String endpointURL = "http://lod.openlinksw.com/sparql/";
        Repository remoteSPARQL = new SPARQLRepository(endpointURL);

        remoteSPARQL.initialize();

        RepositoryConnection remoteconn =
                remoteSPARQL.getConnection();
        try {
            String sparqlQuery =
                    "SELECT ?s,?p,?o WHERE {\n" +
                            "  <http://www.w3.org/People/Berners-Lee/card#i> ?p ?o .\n" +
                            "}";
            TupleQuery tupleQuery = remoteconn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            tupleQuery.evaluate(new TupleQueryResultHandler() {
                @Override
                public void startQueryResult(List<String> bindingNames) {
                }

                @Override
                public void handleSolution(BindingSet bindingSet) {
                    Resource subject = f.createIRI("http://www.w3.org/People/Berners-Lee/card#i");
                    Statement st = conn.getValueFactory().createStatement(subject,(IRI) bindingSet.getValue("p"), bindingSet.getValue("o"));
                    try {
                        conn.add(st, context1);
                    } catch (RepositoryException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void endQueryResult() {
                }

                @Override
                public void handleBoolean(boolean arg0)
                        throws QueryResultHandlerException {
                }

                @Override
                public void handleLinks(List<String> arg0)
                        throws QueryResultHandlerException {
                }
            });

            String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://www.w3.org/People/Berners-Lee/card#i> <http://data.semanticweb.org/ns/swc/ontology#holdsRole> <http://events.linkeddata.org/ldow2009/#chairrole> .}}";
            BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
            Assert.assertTrue(booleanAliceQuery.evaluate());

            conn.clear(context1);

        }
        finally {
            remoteconn.close();
        }

    }

    // https://github.com/marklogic/marklogic-sesame/issues/66
    @Test
    public void testRemoveStatementIteration()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createIRI("http://marklogic.com/test/context1");
        Resource context2 = f.createIRI("http://marklogic.com/test/context2");
        Resource context3 = f.createIRI("http://marklogic.com/test/context3");
        
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");
        IRI age = f.createIRI("http://example.org/ontology/age");
        Literal alicesAge = f.createLiteral(11);

        Statement st1 = f.createStatement(alice, name, alicesName);
        Statement st2 = f.createStatement(alice, age, alicesAge);

        conn.begin();
        conn.add(st1, context1);
        conn.add(st2, context2);
        conn.add(st1, context3);
        conn.commit();

        Assert.assertEquals(1L, conn.size(context1));

        Iteration<? extends Statement, RepositoryException> iter = conn.getStatements(null, null,
                null, false);
        
        conn.remove(iter, context1);
        Assert.assertEquals(0L, conn.size(context1));
        Assert.assertEquals(1L, conn.size(context1, context2));
        
        iter = conn.getStatements(null, null, null, false);
        conn.remove(iter);
        Assert.assertEquals(0L, conn.size(context1, context2, context3));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/68
    @Test
    public void testGetStatementWithNullContext()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createIRI("http://marklogic.com/test/context1");
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.begin();
        conn.add(st1, context1);
        conn.add(st1);
        conn.commit();

        Assert.assertEquals(1L, conn.size(context1));
        Assert.assertEquals(2L, conn.size(null, context1));

        Iteration<? extends Statement, RepositoryException> iter = conn.getStatements(alice, name,
                null, false, null, context1);

        while (iter.hasNext()) {
            Statement s = iter.next();
            System.out.println(s.getContext());
            assertTrue("All statements in context must be null or 'context1'", s.getContext() == null || s.getContext().equals(context1));
        }
        
        iter = conn.getStatements(alice, name, null, false, context1);
        while (iter.hasNext()) {
            Statement s = iter.next();
            System.out.println(s.getContext());
            assertTrue("All statements in context must be context1'", s.getContext().equals(context1));
        }
        
        iter = conn.getStatements(alice, name, null, false, (Resource) null);
        while (iter.hasNext()) {
            Statement s = iter.next();
            System.out.println(s.getContext());
            assertTrue("All statements in context must be null'", s.getContext() == null);
        }
        conn.clear(context1, null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/61
    @Test
    public void testRemoveWithNullObject()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createIRI("http://marklogic.com/test/context1");
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI age = f.createIRI("http://example.org/ontology/age");
        Literal alicesName = f.createLiteral("Alice");
        Literal alicesAge = f.createLiteral(22);

        Statement st1 = f.createStatement(alice, name, alicesName);
        Statement st2 = f.createStatement(alice, age, alicesAge);
        conn.begin();
        conn.add(st1, context1);
        conn.add(st2, context1);
        conn.commit();

        Assert.assertEquals(2L, conn.size(context1));
        conn.remove(alice, age, null, context1);
        Assert.assertEquals(1L, conn.size(context1));
        conn.remove(alice, null, alicesName, context1);
        Assert.assertEquals(0L, conn.size(context1));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/132
    @Test
    public void testRemoveStatement()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createIRI("http://marklogic.com/test/context1");
        final IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI age = f.createIRI("http://example.org/ontology/age");
        Literal alicesName = f.createLiteral("Alice");
        Literal alicesAge = f.createLiteral(22);

        Statement st1 = f.createStatement(alice, name, alicesName);
        Statement st2 = f.createStatement(alice, age, alicesAge);
        conn.begin();
        conn.add(st1, context1);
        conn.add(st2, context1);
        conn.commit();
        Statement st3 = f.createStatement(alice, name, f.createLiteral(9999));

        conn.remove(st3);
        assertEquals("Remove Statement (no context) should not remove anything.", 2L, conn.size());
    }

    // https://github.com/marklogic/marklogic-sesame/issues/70
    @Test
    public void testAddWithNullContext() throws Exception {
        ValueFactory f= conn.getValueFactory();
        final IRI william = f.createIRI("http://example.org/people/william");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI age = f.createIRI("http://example.org/ontology/age");
        Literal williamName = f.createLiteral("William");
        Literal williamAge = f.createLiteral(22);

        Statement st1 = f.createStatement(william, name, williamName);
        Statement st2 = f.createStatement(william, age, williamAge);

        conn.add(st1, (Resource) null);
        conn.add(st2, (Resource) null);
        conn.sync();
        conn.remove(william, age, williamName, (Resource) null);
        conn.remove(william, name, williamAge, (Resource) null);
        conn.sync();
    }

    //https://github.com/marklogic/marklogic-sesame/issues/139
    @Test
    public void testSizeWithLargerGraph() throws Exception {
        File inputFile = new File(TESTFILE_OWL);
        conn.add(inputFile, null, RDFFormat.RDFXML);
        Assert.assertEquals(449, conn.size());
        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/semantics#default-graph"));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/83
    @Test
    public void testSizeWithNull() throws Exception {
        File inputFile = new File(TESTFILE_OWL);
        conn.add(inputFile, null, RDFFormat.RDFXML);
        Assert.assertEquals(449, conn.size((Resource) null));
        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/semantics#default-graph"));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/120
    @Test
    public void testSizeWithUncastedNull() throws Exception {
        File inputFile = new File(TESTFILE_OWL);
        conn.add(inputFile,null,RDFFormat.RDFXML);
        Assert.assertEquals(449, conn.size(null));
        conn.clear(conn.getValueFactory().createIRI("http://marklogic.com/semantics#default-graph"));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/82
    @Test
    public void testGetStatementWithMultipleContexts() throws Exception{
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context6");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true);

        Model aboutEveryone = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(6L, aboutEveryone.size());

        statements = conn.getStatements(null, null, null, true, context5,context6);
        List<Statement> aboutList = Iterations.asList(statements);

        Assert.assertEquals(6L, aboutList.size()); // TBD- why does it dedupe ?

        conn.clear(context5,context6);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/81
    @Test
    public void testGetStatementReturnCorrectContext() throws Exception{
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context6");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        CloseableIteration<? extends Statement, RepositoryException> iter = conn.getStatements(null, null, null, false, context5);


        while(iter.hasNext()){
            Statement st = iter.next();
            Assert.assertTrue(st.getContext().equals(context5)  || st.getContext().equals(context6));
        }
        conn.clear(context5,context6);
    }


    // https://github.com/marklogic/marklogic-sesame/issues/90
    @Test
    public void testGetStatementIsEqualToSize() throws Exception{
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI bob = f.createIRI("http://example.org/people/bob");
        IRI name = f.createIRI("http://example.org/ontology/name");
        IRI person = f.createIRI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, null, context5);
        conn.add(alice, name, alicesName, null, context5);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5);

        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true, null,context5);
        Model aboutPeople = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(conn.size(null,context5),aboutPeople.size());
        conn.clear(null,context5);
    }

    @Test
    public void testCompareSizeAWithNullContext() throws Exception {
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");

        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context6");

        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);

        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context5);

        File inputFile3 = new File("src/test/resources/testdata/default-graph-3.ttl");
        conn.add(inputFile3, "http://example.org/example1/", RDFFormat.TURTLE, context6);

        Assert.assertEquals(12, conn.size());
        Assert.assertEquals(4, conn.size((Resource) null));
        Assert.assertEquals(4, conn.size(context5));
        Assert.assertEquals(8, conn.size((Resource) null, context5));
        Assert.assertEquals(8, conn.size(context5, (Resource) null, context5));
        Assert.assertEquals(12, conn.size(context6, (Resource) null, context5));
        conn.clear();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/120
    @Test
    public void testSizeAWithNullContext() throws Exception {
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");
        Resource nonexistent = conn.getValueFactory().createIRI("http://marklogic.com/test/nonexistent");

        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, (Resource) null);

        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context5);

        Assert.assertEquals(4, conn.size(null));
        Assert.assertEquals(8, conn.size());
        Assert.assertEquals(4, conn.size(context5));
        Assert.assertEquals(4, conn.size((Resource) null));
        Assert.assertEquals(8, conn.size(context5, null));
        Assert.assertEquals(8, conn.size(null, context5));
        Assert.assertEquals(0, conn.size(nonexistent));
        conn.clear();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/118
    @Test
    public void testAddDeleteInsertWithTransaction()
            throws RDF4JException
    {
        ValueFactory vf= conn.getValueFactory();
        IRI fei = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        IRI email = vf.createIRI("http://marklogicsparql.com/addressbook#email");
        Literal feilname = vf.createLiteral("Ling");
        Literal feiemail = vf.createLiteral("fei.ling@marklogic.com");

        conn.add(fei, lname, feilname);
        conn.add(fei, email, feiemail);
        conn.begin();
        conn.prepareUpdate("" +
                "DELETE { <http://marklogicsparql.com/id#3333> <#email> \"fei.ling@marklogic.com\"} INSERT { <http://marklogicsparql.com/id#3333> <#email> \"fling@marklogic.com\"}" +
                " where{ ?s <#email> ?o}","http://marklogicsparql.com/addressbook").execute();
        conn.commit();
        logger.info(
                "hasStatement:{}", conn.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false)
        );
        Assert.assertTrue("The value of email should be updated", conn.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false));
        Assert.assertFalse(conn.isEmpty());
    }

    // https://github.com/marklogic/marklogic-sesame/issues/131
    @Test
    public void testLiterals()
            throws RDF4JException
    {
        ValueFactory vf= conn.getValueFactory();
        IRI fei = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");

        Literal feilname = vf.createLiteral("Ling", "zh");
        //Literal shouldfail = vf.createLiteral(1, "zh");
        Literal invalidIntegerLiteral = vf.createLiteral("four", XMLSchema.INTEGER);
        Literal feiage = vf.createLiteral(25);

        conn.add(fei, lname, feilname);
        conn.add(fei, age, feiage);
        //conn.add(fei, age, invalidIntegerLiteral);
        logger.info("lang:{}", conn.hasStatement(vf.createStatement(fei, lname, vf.createLiteral("Ling", "zh")), false));
        logger.info("size:{}", conn.size());
        Assert.assertFalse("The lang tag of lname is not en", conn.hasStatement(vf.createStatement(fei, lname, vf.createLiteral("Ling", "en")), false));
        Assert.assertTrue("The lang tag of lname is zh", conn.hasStatement(vf.createStatement(fei, lname, feilname), false));
        Assert.assertFalse(conn.isEmpty());
    }

    @Test
    public void testLiteralsWithNonexistantLangTag()
            throws RDF4JException
    {
        ValueFactory vf= conn.getValueFactory();
        IRI fei = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        Literal feilname = vf.createLiteral("Ling", "nonexistentlangtag");
        conn.add(fei, lname, feilname);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/133
    @Test
    public void testAddRemoveInsert()
            throws RDF4JException {
        ValueFactory vf = conn.getValueFactory();
        IRI tommy = vf.createIRI("http://marklogicsparql.com/id#4444");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        Literal tommylname = vf.createLiteral("Ramone");
        Statement stmt = vf.createStatement(tommy, lname, tommylname);
        conn.add(stmt);
        try {
            conn.begin();
            conn.remove(stmt);
            Assert.assertEquals("The size of repository must be zero", 0, conn.size());
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            conn.commit();
        }

    }

    // https://github.com/marklogic/marklogic-sesame/issues/250
    @Test
    public final void exportEmptyStore()
            throws RDF4JException
    {
        IRI dirgraph = conn.getValueFactory().createIRI("http://marklogic.com/dirgraph");

        Assert.assertEquals(0L, conn.size());
        conn.exportStatements(null, null, null, false, new RDFHandlerBase() {

            @Override
            public void handleStatement(Statement st1)
                    throws RDFHandlerException {
                Assert.assertNull(st1);
            }

        }, dirgraph);
    }


    @Test
    public void testNestedConnections()
            throws RDF4JException {

        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig();
        config.setHost(host);
        config.setPort(port);
        config.setUser(user);
        config.setPassword(password);
        config.setAuth("DIGEST");

        MarkLogicRepositoryFactory FACTORY = new MarkLogicRepositoryFactory();

        ValueFactory vf = conn.getValueFactory();
        IRI tommy = vf.createIRI("http://marklogicsparql.com/id#4444");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        Literal tommylname = vf.createLiteral("Ramone");
        Statement stmt = vf.createStatement(tommy, lname, tommylname);

        conn.add(stmt);

        conn.begin();

        Repository repo2 = FACTORY.getRepository(config);
        repo2.initialize();
        RepositoryConnection conn2 = repo2.getConnection();

        conn2.begin();
        Assert.assertEquals("The size of repository must be zero", 1, conn.size());
        conn2.commit();

        conn2.close();
        repo2.shutDown();

        conn.commit();

    }
    
    // duplicated functional test
    @Test
    public void testAddDeleteAdd()
            throws RDF4JException
    {
        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");
        ValueFactory f= conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st = f.createStatement(alice, name, alicesName, context5);

        conn.add(st);
        conn.begin();
        String defGraphQuery =  "DELETE DATA {GRAPH <" + context5.stringValue()+ "> { <" + alice.stringValue() + "> <" + name.stringValue() + "> \"" + alicesName.stringValue() + "\"^^<http://www.w3.org/2001/XMLSchema#string>} }";
        conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery).execute();
        Assert.assertTrue(conn.isEmpty());
        conn.add(st);
        conn.commit();
        Assert.assertFalse(conn.isEmpty());
        conn.remove(st);
    }

    //https://github.com/marklogic/marklogic-sesame/issues/362
    @Test
    public void testContextWhenSuppliedExplicitly()
    {
        Resource context6 = conn.getValueFactory().createIRI("http://marklogic.com/test/context6");
        ValueFactory f = conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st = f.createStatement(alice, name, alicesName, context6);

        Resource context7 = conn.getValueFactory().createIRI("http://marklogic.com/test/context7");
        conn.add(st, context7);

        String queryString = "ASK {GRAPH <" + context7.stringValue() + "> {<" + alice.stringValue() + "> <" + name.stringValue() +"> \"" + alicesName.stringValue() + "\"^^<http://www.w3.org/2001/XMLSchema#string>}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        Assert.assertTrue(booleanQuery.evaluate());

        queryString = "ASK {GRAPH <" + context6.stringValue() + "> {<" + alice.stringValue() + "> <" + name.stringValue() +"> \"" + alicesName.stringValue() + "\"^^<http://www.w3.org/2001/XMLSchema#string>}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        Assert.assertFalse(booleanQuery.evaluate());
    }
}

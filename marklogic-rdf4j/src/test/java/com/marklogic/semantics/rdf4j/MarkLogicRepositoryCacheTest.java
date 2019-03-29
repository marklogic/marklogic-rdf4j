/*
 * Copyright 2015-2019 MarkLogic Corporation
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

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.StatementImpl;
import org.eclipse.rdf4j.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * tests write cache
 *
 *
 */
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MarkLogicRepositoryCacheTest extends Rdf4jTestBase {

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
        if(conn.isActive() && conn.isOpen()){conn.rollback();}
        if(conn.isOpen()){conn.clear();}
        conn.close();
        conn = null;
        rep.shutDown();
        rep = null;
        logger.info("tearDown complete.");
    }

    // https://github.com/marklogic/marklogic-sesame/issues/140
    // https://github.com/marklogic/marklogic-sesame/issues/183
    @Test
    public void testStatementWithWriteCache() throws Exception {
        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");

        ValueFactory f= conn.getValueFactory();

        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName, context1);
        conn.add(st1);
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

        assertEquals("Incorrect number of triples.", 15001, conn.size());
        conn.clear();
    }

    @Test
    public void testSizeCommitWithWriteCache()
            throws Exception
    {
        conn.setIsolationLevel(IsolationLevels.SNAPSHOT);

        assertEquals(conn.size(),0L);

        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        IRI alice = f.createIRI("http://example.org/people/alice");
        IRI name = f.createIRI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName, context1);

        try{
            conn.begin();
            conn.add(st1);
            assertEquals(conn.size(),1L);
            conn.commit();
        }
        catch (Exception e) {
        }
        finally{
            if (conn.isActive())
                conn.rollback();

        }
        assertEquals(conn.size(),1L);
        conn.clear();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/241
    @Test
    public void testLarge()
            throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        IRI graph = vf.createIRI("urn:test");
        int docSize = 100000;
        conn.configureWriteCache(100,500,300);
        conn.begin();
        Set<Statement> bulkInsert = new HashSet();
        for (int term = 0; term < docSize; term++) {
            bulkInsert.add(vf.createStatement
                    (vf.createIRI("urn:subject:" + term),
                            vf.createIRI("urn:predicate:" + term),
                            vf.createIRI("urn:object:" + term)));
        }
        conn.add(bulkInsert, graph);
        conn.commit();
        assertEquals(100000L, conn.size());
//        RepositoryResult stmts = conn.getStatements(null,null,null,false,graph);
//        conn.remove(stmts);
//        assertEquals(0L, conn.size());
    }

    @Test
    @Ignore
    // This test is very long-running, ignore in general test runs.
    public void testLargeTrig() throws Exception
    {
        //File size ~ 277 MB
        String fileName = "src/test/resources/testdata/bigTrig.trig";
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));

        String alice = "http://example.org/people/alice";
        String name = "http://example.org/ontology/name";
        String aliceName = "Alice";
        for (long i=0; i<3000000; i++)
        {
            bufferedWriter.write("<" + alice + i +"> <" + name + "> \"" + aliceName + i + "\" .\n");
        }
        bufferedWriter.close();

        File file = new File(fileName);
        conn.begin();
        conn.add(file, null, RDFFormat.TRIG);
        file.delete();
        conn.commit();

        ValueFactory vf = SimpleValueFactory.getInstance();
        Literal alicesName1 = vf.createLiteral("Alice1");
        Literal alicesName2999999 = vf.createLiteral("Alice2999999");
        Assert.assertTrue(conn.hasStatement(null, null, alicesName1, false));
        Assert.assertTrue(conn.hasStatement(null, null, alicesName2999999, false));
    }

}

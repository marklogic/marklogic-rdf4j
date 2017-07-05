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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.SesameTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * test UpdateQuery
 *
 * @author James Fuller
 */
public class MarkLogicUpdateQueryTest extends SesameTestBase {

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
        gmgr.write("/directory1/test.rdf", new FileHandle(testData));
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
        gmgr.delete("/directory1/test.rdf");
    }

    @Test
    public void testUpdateQuery()
            throws Exception {
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/test> <pp1> <oo1> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/test/g27"));
    }

    @Test
    public void testUpdateQueryWithBaseURI()
            throws Exception {
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/example/context1> {  <http://marklogic.com/test/subject> <relative1> <relative2> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/test/subject> <relative1> <relative2> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery,"http://marklogic.com/test/baseuri");
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery,"http://marklogic.com/test/baseuri");
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/example/context1"));
    }

    @Test
    public void testUpdateQueryWithExistingBaseURI()
            throws Exception {
        String defGraphQuery = "BASE <http://marklogic.com/test/baseuri> INSERT DATA { GRAPH <http://marklogic.com/test/context10> {  <http://marklogic.com/test/subject> <pp1> <oo1> } }";
        String checkQuery = "BASE <http://marklogic.com/test/baseuri> ASK WHERE { <http://marklogic.com/test/subject> <pp1> <oo1> }";
        MarkLogicUpdateQuery updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery,"http://marklogic.com/test/baseuri");
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/test/context10"));
    }

    @Test(expected=org.eclipse.rdf4j.query.UpdateExecutionException.class)
    public void testUpdateQueryUpdateExecutionException()
            throws Exception {
        String defGraphQuery = "INSERT DATA GRAPH <http://marklogic.com/test/g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
    }

    @Test(expected=org.eclipse.rdf4j.query.UpdateExecutionException.class)
    public void testUpdateQueryMalformedException()
            throws Exception {
        String defGraphQuery = "INSERT1 DATA GRAPH <http://marklogic.com/test/g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
    }

}

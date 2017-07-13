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
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * Sesame API.
 */
package com.marklogic.semantics.rdf4j;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tests MarkLogicRespository
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryTest extends Rdf4jTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testRepoInitialized()
            throws Exception {

        logger.info("setting up repo");
        rep.initialize();

        Assert.assertTrue(rep instanceof Repository);
        rep.shutDown();
    }

    @Test
    public void negativeTestRepo2()
            throws Exception {

        Repository rep = new MarkLogicRepository(host, port, user, password, "DIGEST");
        rep.initialize();
        rep.shutDown();

        // should throw error as we shutdown repo
        exception.expect(RepositoryException.class);
        exception.expectMessage("MarkLogicRepository not initialized.");
        @SuppressWarnings("unused")
        RepositoryConnection conn = rep.getConnection();
    }

    @Test
    public void testRepo()
            throws Exception {

        Repository rep = new MarkLogicRepository(host, port, user, password, "DIGEST");
        rep.initialize();
        rep.shutDown();

        rep.initialize();
        Assert.assertTrue(rep.getDataDir() == null);
        Assert.assertTrue(rep.isWritable());
        Assert.assertTrue(rep.getValueFactory() instanceof SimpleValueFactory);
        Assert.assertTrue(rep.getConnection() instanceof MarkLogicRepositoryConnection);
    }

    @Test
    public void testRepo2()
            throws Exception {

        MarkLogicRepository rep = new MarkLogicRepository(host, port, user, password, "DIGEST");
        rep.initialize();
        rep.shutDown();

        rep.initialize();
        Assert.assertTrue(rep.getDataDir() == null);
        Assert.assertTrue(rep.isWritable());
        Assert.assertTrue(rep.getValueFactory() instanceof SimpleValueFactory);
        Assert.assertTrue(rep.getConnection() instanceof MarkLogicRepositoryConnection);
    }

    @Test
    public void TestRepoWithJavaAPIClientDatabaseClient()
            throws Exception {
        DatabaseClient databaseClient = DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.DigestAuthContext(user, password));
        Repository rep = new MarkLogicRepository(databaseClient);
        rep.initialize();
        Assert.assertTrue(rep instanceof Repository);
        RepositoryConnection conn = rep.getConnection();
        Assert.assertTrue(conn instanceof RepositoryConnection);
        conn.close();
        rep.shutDown();
    }

    @Test
    public void testMultipleReposWithDifferentUsers() throws RepositoryException, MalformedQueryException, UpdateExecutionException {
        readerRep.initialize();
        MarkLogicRepositoryConnection testReaderCon = readerRep.getConnection();
        exception.expect(Exception.class);
        testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();

        writerRep.initialize();
        MarkLogicRepositoryConnection testWriterCon = writerRep.getConnection();
        testWriterCon.prepareUpdate("CREATE GRAPH <abcdef10>").execute();
        writerRep.shutDown();
        readerRep.shutDown();
    }
}

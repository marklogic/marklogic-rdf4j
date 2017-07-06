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
package com.marklogic.semantics.rdf4j.config;

import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;
import org.eclipse.rdf4j.sail.memory.config.MemoryStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.marklogic.semantics.rdf4j.Rdf4jTestBase.*;

/**
 * test factory
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryManagerTest {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testLocalManager() throws Exception {
        RepositoryManager manager;
        manager = new LocalRepositoryManager(new File("/tmp/localmanager"));
        manager.initialize();
        RepositoryConfig mlconf = new RepositoryConfig("jimtest1",new MarkLogicRepositoryConfig(host, port, user, password, "DIGEST"));

        manager.addRepositoryConfig(new RepositoryConfig("test", new SailRepositoryConfig(
                new MemoryStoreConfig(true))));
        manager.addRepositoryConfig(mlconf);

        MarkLogicRepository mlrepo = (MarkLogicRepository)manager.getRepository("jimtest1");
        mlrepo.initialize();
        MarkLogicRepositoryConnection mlconn = mlrepo.getConnection();
        ValueFactory vf = mlconn.getValueFactory();
        URI tommy = vf.createURI("http://marklogicsparql.com/id#4444");
        URI lname = vf.createURI("http://marklogicsparql.com/addressbook#lastName");
        Literal tommylname = vf.createLiteral("Ramone");
        Statement stmt = vf.createStatement(tommy, lname, tommylname);
        mlconn.begin();
        mlconn.add(stmt);
        mlconn.commit();
        Assert.assertEquals(1, mlconn.size());

        mlconn.clear();
    }

    @Ignore
    @Test
    // requires a Sesame server to be running eg. RemoteRepositoryManager uses HTTPRepository
    public void testRemoteManager() throws Exception {
        RepositoryManager manager;
        manager = new RemoteRepositoryManager("http://localhost:8080/eclipse.rdf4j-sesame");
        manager.initialize();
        RepositoryConfig mlconf = new RepositoryConfig("remotetest",new MarkLogicRepositoryConfig("localhost", 8200, "admin", "admin", "DIGEST"));

        manager.addRepositoryConfig((RepositoryConfig) mlconf);

        Repository mlrepo = manager.getRepository("remotetest");

        mlrepo.initialize();
        RepositoryConnection mlconn = mlrepo.getConnection();

        ValueFactory vf = mlconn.getValueFactory();
        URI tommy = vf.createURI("http://marklogicsparql.com/id#4444");
        URI lname = vf.createURI("http://marklogicsparql.com/addressbook#lastName");
        Literal tommylname = vf.createLiteral("Ramone");
        Statement stmt = vf.createStatement(tommy, lname, tommylname);
        mlconn.begin();
        mlconn.add(stmt);
        mlconn.commit();

        mlconn.clear();
    }
}
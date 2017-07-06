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

import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.marklogic.semantics.rdf4j.Rdf4jTestBase.*;

/**
 * test factory
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryFactoryTest {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetRepository() throws Exception {
        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig();
        config.setHost(host);
        config.setPort(port);
        config.setUser(adminUser);
        config.setPassword(adminPassword);
        config.setAuth("DIGEST");

        RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());

        Repository repo = factory.getRepository(config);
        repo.initialize();
        Assert.assertTrue(repo.getConnection() instanceof MarkLogicRepositoryConnection);

        Repository otherrepo = factory.getRepository(config);
        otherrepo.initialize();
        RepositoryConnection oconn = otherrepo.getConnection();
        Assert.assertTrue(oconn instanceof MarkLogicRepositoryConnection);
    }

    @Test
    public void testGetRepositoryWithAllInOneConstructor() throws Exception {
        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig(host, port, user, password, "DIGEST");

        RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());

        Repository repo = factory.getRepository(config);
        repo.initialize();
        Assert.assertTrue(repo.getConnection() instanceof MarkLogicRepositoryConnection);

        Repository otherrepo = factory.getRepository(config);
        otherrepo.initialize();
        Assert.assertTrue(otherrepo.getConnection() instanceof RepositoryConnection);
    }
}
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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.semantics.rdf4j.client.MarkLogicClientDependent;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import com.marklogic.semantics.rdf4j.utils.Util;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 *
 * RDF4J repository representing a MarkLogic triple store,
 * exposing MarkLogic-specific features; SPARQL and Graph queries
 * in all SPARQL forms, rulesets for inferencing, efficient
 * size queries, combination queries,  base uri, and permissions.
 *
 */
public class MarkLogicRepository extends AbstractRepository implements Repository, MarkLogicClientDependent {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepository.class);

    // DatabaseClient vars
    private String host;
    private int port;
    private String user;
    private String password;
    private String auth;
    private String database;
    private DatabaseClientFactory.SecurityContext securityContext;

    private boolean quadMode;

    private ValueFactory f;

    private DatabaseClient databaseClient;
    private boolean externalClient = true;

    private Util util = Util.getInstance();


    /**
     * Constructor initialized with connection URL
     *
     * @param connectionString
     */
    public MarkLogicRepository(URL connectionString){
        String[] split = connectionString.getAuthority().split("@");
        String creds = split[0];
        String cred[] = creds.split(":");
        this.f = SimpleValueFactory.getInstance();
        this.quadMode = true;
        this.host = connectionString.getHost();
        this.port = connectionString.getPort();
        this.user = cred[0];
        this.password = cred[1];
        this.auth = "DIGEST";
        this.database = null;
        makeDatabaseClient();
    }
    /**
     *
     * Constructor initialized with connection vars to MarkLogic server.
     *
     * @param host the host with the REST server
     * @param port the port for the REST server
     * @param user the user with read, write, or administrative privileges
     * @param password the password for the user
     * @param auth the type of authentication applied to the request
     * @deprecated since 1.1.0 use {@link MarkLogicRepository#MarkLogicRepository(java.lang.String, int, com.marklogic.client.DatabaseClientFactory.SecurityContext)} instead.
     */
    @Deprecated
    public MarkLogicRepository(String host, int port, String user, String password, String auth) {
        this(host, port, user, password, null, auth);
    }
    /**
     *
     * Constructor initialized with connection vars to MarkLogic server.
     *
     * @param host the host with the REST server
     * @param port the port for the REST server
     * @param user the user with read, write, or administrative privileges
     * @param password the password for the user
     * @param auth the type of authentication applied to the request
     * @param database the MarkLogic database to be used.
     * @deprecated since 1.1.0 use {@link MarkLogicRepository#MarkLogicRepository(java.lang.String, int, java.lang.String, com.marklogic.client.DatabaseClientFactory.SecurityContext)} instead.
     */
    @Deprecated
    public MarkLogicRepository(String host, int port, String user, String password, String database, String auth) {
        super();
        this.f = SimpleValueFactory.getInstance();
        this.quadMode = true;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.auth = auth;
        this.database = database;
        makeDatabaseClient();
    }
    /**
     *
     * Constructor initialized with connection vars to MarkLogic server.
     *
     * @param host the host with the REST server
     * @param port the port for the REST server
     * @param securityContext a Java Client API SecurityContext. Can be made with com.marklogic.client.DatabaseClientFactory
     */
    public MarkLogicRepository(String host, int port, DatabaseClientFactory.SecurityContext securityContext) {
        this(host, port, null, securityContext);
    }
    /**
     *
     * Constructor initialized with connection vars to MarkLogic server.
     *
     * @param host the host with the REST server
     * @param port the port for the REST server
     * @param database the MarkLogic database to be used.
     * @param securityContext a Java Client API SecurityContext. Can be made with com.marklogic.client.DatabaseClientFactory
     */
    public MarkLogicRepository(String host, int port, String database, DatabaseClientFactory.SecurityContext securityContext) {
        super();
        this.f = SimpleValueFactory.getInstance();
        this.quadMode = true;
        this.host = host;
        this.port = port;
        this.database = database;
        this.securityContext = securityContext;
        makeDatabaseClient();
    }
    /**
     * Constructor initialized with MarkLogic Java Client Api DatabaseClient.
     *
     * @param databaseClient a Java Client API DatabaseClient. Can be made with com.marklogic.client.DatabaseClientFactory
     */
    public MarkLogicRepository(DatabaseClient databaseClient) {
        super();
        this.f = SimpleValueFactory.getInstance();
        this.quadMode = true;
        this.host = databaseClient.getHost();
        this.port = databaseClient.getPort();
        this.database = databaseClient.getDatabase();
        this.securityContext = databaseClient.getSecurityContext();
        this.databaseClient = databaseClient;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (this.databaseClient != null) {
            if (!this.externalClient) {
                this.databaseClient.release();
            }
            this.databaseClient = null;
        }
        this.f = null;
        this.quadMode = false;
        this.host = null;
        this.port = 0;
        this.database = null;
        this.securityContext = null;
    }

    /**
     * gets the Valuefactory used for creating URIs, blank nodes, literals and statements.
     *
     * @return ValueFactory
     */
    public ValueFactory getValueFactory() {
        return this.f;
    }

    /**
     * sets the ValueFactory used for creating URIs, blank nodes, literals and statements
     *
     * @param f the ValueFactory to be set.
     */
    public void setValueFactory(ValueFactory f) {
        this.f=f;
    }

    /**
     * @deprecated
     * implemented to honor Repository interface
     *
     * @throws RepositoryException
     */
    @Override
    @Deprecated
    protected void initializeInternal() throws RepositoryException
    {
        if (this.databaseClient == null) {
            makeDatabaseClient();
        }
    }

    /**
     * @deprecated
     * Implemented to honor Repository interface.
     *
     * @throws RepositoryException
     */
    @Override
    @Deprecated
    protected void shutDownInternal() throws RepositoryException {
        if (databaseClient != null) {
            if (!this.externalClient) {
                databaseClient.release();
            }
            databaseClient = null;
        }
    }

    /**
     * MarkLogic has no concept of data directory, so this returns null
     * implemented to honor Repository interface
     *
     * @return always returns null
     */
    @Override
    public File getDataDir() {
        return null;
    }

    /**
     * MarkLogic has no concept of data directory, so this does nothing.
     * Implemented to honor Repository interface.
     *
     * @param dataDir
     */
    @Override
    public void setDataDir(File dataDir) {
        // originally implemented to honor repository interface
    }

    /**
     * MarkLogic, with the correct permissions is always writable.
     * Implemented to honor Repository interface.
     *
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean isWritable() throws RepositoryException {
        return true;
    }

    /**
     * Returns a MarkLogicConnection object which is the entry point to
     * performing all queries.
     *
     * It is best practice to reuse a single connection to a
     * single MarkLogic database to take advantage of connection
     * pooling capabilities built into java api client (which is a
     * dependency within marklogic-rdf4j).
     *
     * @return MarkLogicRepositoryConnection
     * @throws RepositoryException
     */
    @Override
    public MarkLogicRepositoryConnection getConnection()
            throws RepositoryException {
        if (!isInitialized()) {
            throw new RepositoryException("MarkLogicRepository not initialized.");
        }
        return new MarkLogicRepositoryConnection(this, makeMarkLogicClient(), quadMode);
    }

    /**
     * Returns MarkLogicClient object which manages communication to ML server via Java api client
     *
     * @return MarkLogicClient
     */
    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        return makeMarkLogicClient();
    }
    private synchronized MarkLogicClient makeMarkLogicClient() {
        if (this.databaseClient == null) {
            makeDatabaseClient();
        }
        return new MarkLogicClient(this.databaseClient);
    }
    private void makeDatabaseClient() {
        if (this.securityContext == null) {
            this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.user, this.password, this.database, this.auth);
            this.securityContext = databaseClient.getSecurityContext();
        } else {
            this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.database, this.securityContext);
        }
        this.externalClient = false;
    }

    /**
     * This setter is now a noop because each repository connection has a separate MarkLogicClient.
     *
     * This method and will be removed at a future date.
     *
     * @param client the MarkLogicClient to be used, which mediates all the interactions with the MarkLogic database.
     */
    @Deprecated
    public void setMarkLogicClient(MarkLogicClient client) {
    }

    /**
     * Returns if repository is in quadmode or not.
     *
     * @return boolean
     */
    public boolean isQuadMode() {
        return quadMode;
    }

    /**
     * Sets quadmode for this repository.
     *
     * @param quadMode
     */
    public void setQuadMode(boolean quadMode) {
        this.quadMode = quadMode;
    }

}
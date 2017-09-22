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
 * @author James Fuller
 * @version 1.0.0
 *
 */
public class MarkLogicRepository extends AbstractRepository implements Repository,MarkLogicClientDependent {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepository.class);

    // MarkLogicClient vars
    private MarkLogicClient client;
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
        this.client = getMarkLogicClient();
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
     */
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
     * @param database
     */
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
        this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.user, this.password, this.database, this.auth);
        this.client = new MarkLogicClient(databaseClient);
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
     * @param database
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
        this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.database, this.securityContext);
        this.client = new MarkLogicClient(databaseClient);
    }

    /**
     * Constructor.
     *
     * @param databaseClient a Java Client API DatabaseClient. Can be made with com.marklogic.client.DatabaseClientFactory
     */
    public MarkLogicRepository(DatabaseClient databaseClient) {
        super();
        this.f = SimpleValueFactory.getInstance();
        this.databaseClient = databaseClient;
        this.quadMode = true;
        this.host = databaseClient.getHost();
        this.port = databaseClient.getPort();
        this.database = databaseClient.getDatabase();
        this.securityContext = databaseClient.getSecurityContext();
        this.client = new MarkLogicClient(databaseClient);
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
        if(this.databaseClient == null || this.client == null || this.databaseClient.getClientImplementation() == null)
        {
            if(this.securityContext == null)
            {
                this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.user, this.password, this.database, this.auth);
                this.client = new MarkLogicClient(databaseClient);
            }
            else
            {
                this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.database, this.securityContext);
                this.client = new MarkLogicClient(databaseClient);
            }
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
        client.release();
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
        return new MarkLogicRepositoryConnection(this, getMarkLogicClient(), quadMode);
    }

    //TODO: Check and refactor.
    /**
     * Returns MarkLogicClient object which manages communication to ML server via Java api client
     *
     * @return MarkLogicClient
     */
    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        if(this.securityContext == null)
        {
            this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.user, this.password, this.database, this.auth);
            this.client = new MarkLogicClient(databaseClient);
        }
        else
        {
            this.databaseClient = util.getClientBasedOnAuth(this.host, this.port, this.database, this.securityContext);
            this.client = new MarkLogicClient(databaseClient);
        }
        return this.client;
    }

    /**
     * Sets MarkLogicClient used by this repository.
     *
     * @param client the MarkLogicClient to be used, which mediates all the interactions with the MarkLogic database.
     */
    @Override
    public synchronized void setMarkLogicClient(MarkLogicClient client) {
        this.client = client;
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
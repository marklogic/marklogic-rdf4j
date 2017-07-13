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
import com.marklogic.semantics.rdf4j.client.MarkLogicClientDependent;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 *
 * Sesame repository representing a MarkLogic triple store,
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

    private boolean quadMode;

    private ValueFactory f;

    private DatabaseClient databaseClient;

    /**
     * Authentication enumerates the methods for verifying a user and
     * password with the database.
     */
    public enum Authentication {
        /**
         * Minimal security unless used with SSL.
         */
        BASIC,
        /**
         * Moderate security without SSL.
         */
        DIGEST,
        /**
         * Authentication using Kerberos.
         */
        KERBEROS,
        /**
         * Authentication using Certificates;
         */
        CERTIFICATE;
        /**
         * Returns the enumerated value for the case-insensitive name.
         * @param name	the name of the enumerated value
         * @return	the enumerated value
         */
        static public Authentication valueOfUncased(String name) {
            return Authentication.valueOf(name.toUpperCase());
        }
    }

    /**
     * constructor inited with connection URL
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
     * constructor inited with connection vars to MarkLogic server
     *
     * @param host
     * @param port
     * @param user
     * @param password
     * @param auth
     */
    public MarkLogicRepository(String host, int port, String user, String password, String auth) {
        super();
        this.f = SimpleValueFactory.getInstance();
        this.quadMode = true;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.auth = auth;
        try {
            this.databaseClient = getClientBasedOnAuth(host, port, user, password, auth);
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.client = new MarkLogicClient(databaseClient);
    }

    /**
     * constructor inited with java api client DatabaseClient
     *
     * @param databaseClient
     */
    public MarkLogicRepository(DatabaseClient databaseClient) {
        super();
        this.f = SimpleValueFactory.getInstance();

        this.databaseClient = databaseClient;
        this.quadMode = true;
        this.host = databaseClient.getHost();
        this.port = databaseClient.getPort();

        if(databaseClient.getSecurityContext() instanceof DatabaseClientFactory.BasicAuthContext)
        {
            DatabaseClientFactory.BasicAuthContext sc = (DatabaseClientFactory.BasicAuthContext) databaseClient.getSecurityContext();
            this.user = sc.getUser();
            this.password = sc.getPassword();
            this.auth = Authentication.BASIC.toString();
        }
        else if(databaseClient.getSecurityContext() instanceof DatabaseClientFactory.DigestAuthContext)
        {
            DatabaseClientFactory.DigestAuthContext sc = (DatabaseClientFactory.DigestAuthContext) databaseClient.getSecurityContext();
            this.user = sc.getUser();
            this.password = sc.getPassword();
            this.auth = Authentication.DIGEST.toString();
        }
        else if(databaseClient.getSecurityContext() instanceof DatabaseClientFactory.KerberosAuthContext)
        {
            DatabaseClientFactory.KerberosAuthContext sc = (DatabaseClientFactory.KerberosAuthContext) databaseClient.getSecurityContext();
            this.user = null;
            this.password = null;
            this.auth = Authentication.KERBEROS.toString();
        }
        else if(databaseClient.getSecurityContext() instanceof DatabaseClientFactory.CertificateAuthContext)
        {
            DatabaseClientFactory.CertificateAuthContext sc = (DatabaseClientFactory.CertificateAuthContext) databaseClient.getSecurityContext();
            this.user = null;
            this.password = null;
            this.auth = Authentication.CERTIFICATE.toString();
        }

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
     * @param f
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
        try {
            this.databaseClient = getClientBasedOnAuth(this.host, this.port, this.user, this.password, this.auth);
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.client = new MarkLogicClient(databaseClient);
    }

    /**
     * @deprecated
     * implemented to honor Repository interface
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
     * MarkLogic has no concept of data directory, so this does nothing
     * implemented to honor Repository interface
     *
     * @param dataDir
     */
    @Override
    public void setDataDir(File dataDir) {
        // originally implemented to honor repository interface
    }

    /**
     * MarkLogic, with the correct perms is always writeable
     * implemented to honor Repository interface
     *
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean isWritable() throws RepositoryException {
        return true;
    }

    /**
     * returns a MarkLogicConnection object which is the entry point to
     * performing all queries.
     *
     * It is best practice to reuse a single connection to a
     * single MarkLogic database to take advantage of connection
     * pooling capabilities built into java api client (which is a
     * dependency within marklogic-sesame).
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

    /**
     * returns MarkLogicClient object which manages communication to ML server via Java api client
     *
     * @return MarkLogicClient
     */
    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        if(null != databaseClient){
            this.client = new MarkLogicClient(databaseClient);
        }else{
            this.client = new MarkLogicClient(host, port, user, password, auth);
        }
        return this.client;
    }

    /**
     * sets MarkLogicClient used by this repository
     *
     * @param client
     */
    @Override
    public synchronized void setMarkLogicClient(MarkLogicClient client) {
        this.client = client;
    }

    /**
     * returns if repository is in quadmode or not
     *
     * @return boolean
     */
    public boolean isQuadMode() {
        return quadMode;
    }

    /**
     * sets quadmode for this repository
     *
     * @param quadMode
     */
    public void setQuadMode(boolean quadMode) {
        this.quadMode = quadMode;
    }


    /**
     * returns client based on auth
     *
     * @param auth
     * @return DatabaseClient
     */
    private DatabaseClient getClientBasedOnAuth(String host, int port, String user, String password, String auth, String... cert) throws UnrecoverableKeyException, CertificateException, KeyManagementException, IOException {
        Authentication type;
        String certFile;
        String certPassword;

        if(auth != null)
        {
            type = Authentication.valueOfUncased(auth);
            certFile = cert.length > 0 ? cert[0] : "";
            certPassword = cert.length > 1 ? cert[1] : "";

            if(type == Authentication.BASIC)
            {
                return DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.BasicAuthContext(user, password));
            }
            else if(type == Authentication.DIGEST)
            {
                return DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.DigestAuthContext(user, password));
            }
            else if(type == Authentication.KERBEROS)
            {
                return DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.KerberosAuthContext());
            }
            else if(type == Authentication.CERTIFICATE)
            {
                //TODO: change parameters
                return DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.CertificateAuthContext(certFile, certPassword));
            }
        }

        return null;
    }
}
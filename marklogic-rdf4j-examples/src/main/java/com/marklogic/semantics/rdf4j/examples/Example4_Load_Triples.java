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
package com.marklogic.semantics.rdf4j.examples;

import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.StatementImpl;
import org.eclipse.rdf4j.model.impl.URIImpl;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;


public class Example4_Load_Triples {

    protected static final Logger logger = LoggerFactory.getLogger(Example4_Load_Triples.class);

    MarkLogicRepository repo;
    MarkLogicRepositoryConnection conn;

    public Example4_Load_Triples() throws RepositoryException {
        System.out.println("setup");
        this.repo = Setup.loadPropsAndInit(); // invoke new MarkLogicRepository(host,port,user,pass,"DIGEST");
        this.repo.initialize(); // initialise repository
        this.conn = repo.getConnection(); // get a repository connection
    }

    public void teardown() throws RepositoryException {
        System.out.println("teardown");
        this.conn.close(); // close connection
        this.repo.shutDown();
    }

    public void loadTriples() throws RepositoryException {
        ValueFactory vf = SimpleValueFactory.getInstance();
        IRI graph = vf.createIRI("urn:test");
        int docSize = 100000;

        conn.configureWriteCache(750,750,600); // customise write cache (initDelay interval, delayCache interval, cache size)

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
    }

    public static void main(String... args) throws RepositoryException {
        System.out.println("instantiate Simple class");
        Example4_Load_Triples simple = new Example4_Load_Triples(); // we instantiate so we can call non static methods
        try {
            logger.info("start examples");
            simple.loadTriples(); //load 100,000 triples
            logger.info("finished examples");
        }finally {
            simple.teardown();
        }
    }
}


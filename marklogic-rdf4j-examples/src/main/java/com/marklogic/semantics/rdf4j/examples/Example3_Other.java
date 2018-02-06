/*
 * Copyright 2015-2018 MarkLogic Corporation
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
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Example3_Other {

    protected static final Logger logger = LoggerFactory.getLogger(Example3_Other.class);

    MarkLogicRepository repo;
    MarkLogicRepositoryConnection conn;

    public Example3_Other() throws RepositoryException {
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

    public void tripleCount() throws RepositoryException {
            System.out.println("number of triples");
            System.out.println(this.conn.size());
    }

    public static void main(String... args) throws RepositoryException {
        System.out.println("instantiate Simple class");
        Example3_Other simple = new Example3_Other(); // we instantiate so we can call non static methods
        try {
            logger.info("start examples");
            simple.tripleCount(); // return number of triples in repo
            logger.info("finished examples");
        }finally {
            simple.teardown();
        }
        System.exit(0);
    }
}


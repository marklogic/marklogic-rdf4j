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
package com.marklogic.rdf4j.functionaltests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;

import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryFactory;
import com.marklogic.rdf4j.functionaltests.util.ConnectedRESTQA;

/**
 * Integration test suite for implementations of Repository.
 * 
 * @author Srinath S
 */
public class MarkLogicRepositoryTest extends  ConnectedRESTQA{
	private static MarkLogicRepository testRepository;
	private static ValueFactory vf ;
	private static String[] hostNames;
	private static MarkLogicRepositoryConnection testConn;
	private static int restPort = 8000;
	private static String host = "localhost";
	private static String dbName = "MLRdf4jRep";
	private static String restServer = "App-Services";
	
	@BeforeClass
	public static void initialSetup() throws Exception {
        hostNames = getHosts();
        createDB(dbName);
        Thread.currentThread().sleep(500L);
        int count = 1;
        for ( String forestHost : hostNames ) {
            createForestonHost(dbName+"-"+count,dbName,forestHost);
            count ++;
            Thread.currentThread().sleep(500L);
        }
        associateRESTServerWithDB(restServer,dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);		
	}
	
	@AfterClass
	public static void tearDownSetup() throws Exception  {
        associateRESTServerWithDB(restServer,"Documents");
        for (int i =0 ; i < hostNames.length; i++){
            detachForest(dbName, dbName+"-"+(i+1));
            deleteForest(dbName+"-"+(i+1));
        }

        deleteDB(dbName);
		
	}
	
	@Before
    public void testGetRepository() throws Exception {
        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig();

        config.setHost(host);
        config.setPort(restPort);
        config.setUser("admin");
        config.setPassword("admin");
        config.setAuth("DIGEST");

        RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
        testRepository = (MarkLogicRepository) factory.getRepository(config);
        testRepository.initialize();
        vf = testRepository.getValueFactory();
        testConn = testRepository.getConnection();
        Assert.assertTrue(testRepository.getConnection() instanceof MarkLogicRepositoryConnection);

    	try{
    		 Repository otherrepo = factory.getRepository(config);
    		 RepositoryConnection conn = otherrepo.getConnection();
			Assert.assertTrue(2>1);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof RepositoryException);
		}
     
    }
	@After
	public void tearDown()
		throws Exception
	{
		clearDB(restPort);
		testConn.close();
		testRepository.shutDown();
		testRepository = null;
		testConn = null;
	}
	@Test
	public void testShutdownFollowedByInit()
		throws Exception
	{
		testRepository.shutDown();
		testRepository.initialize();

		testConn = testRepository.getConnection();
		try {
			Resource s = vf.createIRI("http://a");
			IRI p = vf.createIRI("http://b");
			Value o =vf.createLiteral("c");
			testConn.add(s,p,o);
			assertTrue(testConn.hasStatement(s,p,o, true));
		}
		finally {
			testConn.close();
		}
	}
	
    @Test
    public void testRepo1()
            throws Exception {

    	testRepository.initialize();
        Assert.assertTrue(testRepository.getConnection() instanceof MarkLogicRepositoryConnection);

    	testRepository.shutDown();
    	testRepository.initialize();
    	testRepository.initialize();
    	testRepository.shutDown();
    	testRepository.shutDown();
    	try {
    		testConn = testRepository.getConnection();
			fail("Getting connection object should fail if repository is not initialized");
		}
		catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		}
    	testRepository.initialize();
    	testConn = testRepository.getConnection();
        Assert.assertTrue(testRepository.getDataDir() == null);
        Assert.assertTrue(testRepository.isWritable());
        Assert.assertTrue(testRepository.getValueFactory() instanceof SimpleValueFactory);
    }
	
	
}
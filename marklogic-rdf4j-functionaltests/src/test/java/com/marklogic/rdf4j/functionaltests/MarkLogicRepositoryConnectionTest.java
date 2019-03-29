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
package com.marklogic.rdf4j.functionaltests;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.common.iteration.IteratorIteration;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.UnknownTransactionStateException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.admin.QueryOptionsManager;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.ReaderHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.rdf4j.functionaltests.util.ConnectedRESTQA;
import com.marklogic.rdf4j.functionaltests.util.StatementIterable;
import com.marklogic.rdf4j.functionaltests.util.StatementIterator;
import com.marklogic.rdf4j.functionaltests.util.StatementList;
import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.MarkLogicTransactionException;
import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.rdf4j.config.MarkLogicRepositoryFactory;
import com.marklogic.semantics.rdf4j.query.MarkLogicBooleanQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicTupleQuery;
import com.marklogic.semantics.rdf4j.query.MarkLogicUpdateQuery;

public class MarkLogicRepositoryConnectionTest extends ConnectedRESTQA {

	private static final String TEST_DIR_PREFIX = "/testdata/";
	private static String dbName = "MLRDF4J";
	private static String restServer = "App-Services";
	private static int restPort = 8000;
	private static String[] hostNames;
	private static String host = "localhost";

	protected static DatabaseClient databaseClient;
	protected static MarkLogicRepository testAdminRepository;
	protected static MarkLogicRepository testReaderRepository;
	protected static MarkLogicRepository testWriterRepository;
	protected static MarkLogicRepositoryConnection testAdminCon;
	protected static MarkLogicRepositoryConnection testReaderCon;
	protected static MarkLogicRepositoryConnection testWriterCon;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected ValueFactory vf;
	protected ValueFactory vfWrite;
	protected IRI graph1;
	protected IRI graph2;
	protected IRI dirgraph;
	protected IRI dirgraph1;

	protected IRI john;
	protected IRI micah;
	protected IRI fei;

	protected IRI fname;
	protected IRI lname;
	protected IRI email;
	protected IRI homeTel;

	protected Literal johnfname;
	protected Literal johnlname;
	protected Literal johnemail;
	protected Literal johnhomeTel;
	protected Literal micahfname;
	protected Literal micahlname;
	protected Literal micahhomeTel;
	protected Literal feifname;
	protected Literal feilname;
	protected Literal feiemail;

	protected IRI writeFuncSpecOf;
	protected IRI type;
	protected IRI worksFor;
	protected IRI developPrototypeOf;
	protected IRI ml;
	protected IRI semantics;
	protected IRI inference;
	protected IRI sEngineer;
	protected IRI lEngineer;
	protected IRI engineer;
	protected IRI employee;
	protected IRI design;
	protected IRI subClass;
	protected IRI subProperty;
	protected IRI eqProperty;
	protected IRI develop;
	protected QueryManager qmgr;

	private static final String ID = "id";
	private static final String ADDRESS = "addressbook";
	protected static final String NS = "http://marklogicsparql.com/";
	protected static final String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
	protected static final String OWL = "http://www.w3.org/2002/07/owl#";

	@BeforeClass
	public static void initialSetup() throws Exception {
		hostNames = getHosts();
		createDB(dbName);
		Thread.currentThread().sleep(100L);
		int count = 1;
		for (String forestHost : hostNames) {
			createForestonHost(dbName + "-" + count, dbName, forestHost);
			count++;
			Thread.currentThread().sleep(100L);
		}
		associateRESTServerWithDB(restServer, dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);
		createRESTUser("reader", "reader", "rest-reader");
		createRESTUser("writer", "writer", "rest-writer");
	}

	@AfterClass
	public static void tearDownSetup() throws Exception {
		associateRESTServerWithDB(restServer, "Documents");
		for (int i = 0; i < hostNames.length; i++) {
			detachForest(dbName, dbName + "-" + (i + 1));
			deleteForest(dbName + "-" + (i + 1));
		}

		deleteDB(dbName);
		deleteRESTUser("reader");
		deleteRESTUser("writer");
	}

	@Before
	public void setUp() throws Exception {
		logger.debug("Initializing repository");
		createRepository();

		vf = testAdminCon.getValueFactory();
		vfWrite = testWriterCon.getValueFactory();

		john = vf.createIRI(NS + ID + "#1111");
		micah = vf.createIRI(NS + ID + "#2222");
		fei = vf.createIRI(NS + ID + "#3333");

		fname = vf.createIRI(NS + ADDRESS + "#firstName");
		lname = vf.createIRI(NS + ADDRESS + "#lastName");
		email = vf.createIRI(NS + ADDRESS + "#email");
		homeTel = vf.createIRI(NS + ADDRESS + "#homeTel");

		writeFuncSpecOf = vf.createIRI(NS + "writeFuncSpecOf");
		type = vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		worksFor = vf.createIRI(NS + "worksFor");

		developPrototypeOf = vf.createIRI(NS + "developPrototypeOf");
		ml = vf.createIRI(NS + "MarkLogic");

		semantics = vf.createIRI(NS + "Semantics");
		inference = vf.createIRI(NS + "Inference");
		sEngineer = vf.createIRI(NS + "SeniorEngineer");
		lEngineer = vf.createIRI(NS + "LeadEngineer");
		engineer = vf.createIRI(NS + "Engineer");
		employee = vf.createIRI(NS + "Employee");
		design = vf.createIRI(NS + "design");
		develop = vf.createIRI(NS + "develop");

		subClass = vf.createIRI(RDFS + "subClassOf");
		subProperty = vf.createIRI(RDFS + "subPropertyOf");
		eqProperty = vf.createIRI(OWL + "equivalentProperty");

		johnfname = vf.createLiteral("John");
		johnlname = vf.createLiteral("Snelson");
		johnhomeTel = vf.createLiteral(111111111D);
		johnemail = vf.createLiteral("john.snelson@marklogic.com");

		micahfname = vf.createLiteral("Micah");
		micahlname = vf.createLiteral("Dubinko");
		micahhomeTel = vf.createLiteral(22222222D);

		feifname = vf.createLiteral("Fei");
		feilname = vf.createLiteral("Ling");
		feiemail = vf.createLiteral("fei.ling@marklogic.com");
	}

	@After
	public void tearDown() throws Exception {
		testAdminCon.clear();
		testAdminCon.close();
		testAdminRepository.shutDown();

		testAdminRepository = null;
		testAdminCon = null;

		testReaderRepository.shutDown();
		testReaderRepository = null;
		testReaderCon = null;

		testWriterCon.close();
		testWriterRepository.shutDown();
		testWriterCon = null;
		testWriterRepository = null;

		databaseClient = null;

		logger.info("tearDown complete.");
	}

	/**
	 * Gets an (uninitialized) instance of the repository that should be tested.
	 *
	 * @return void
	 * @throws RepositoryConfigException
	 * @throws RepositoryException
	 */
	protected void createRepository() throws Exception {
		// Creating MLRDF4J Connection object Using MarkLogicRepositoryConfig
		MarkLogicRepositoryConfig adminconfig = new MarkLogicRepositoryConfig();
		adminconfig.setHost(host);
		adminconfig.setAuth("DIGEST");
		adminconfig.setUser("admin");
		adminconfig.setPassword("admin");
		adminconfig.setPort(restPort);
		MarkLogicRepositoryFactory factory = new MarkLogicRepositoryFactory();
		Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
		try {
			testAdminRepository = factory.getRepository(adminconfig);
			testAdminCon = testAdminRepository.getConnection();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		}
		try {
			testAdminRepository.initialize();
			testAdminCon = testAdminRepository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		
		// Creating testAdminCon with MarkLogicRepositoryConfig constructor
		testAdminCon.close();
		testAdminRepository.shutDown();
		testAdminRepository = null;
		testAdminCon = null;

		adminconfig = new MarkLogicRepositoryConfig(host, restPort, "admin", "admin", "DIGEST");
		Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
		testAdminRepository = factory.getRepository(adminconfig);
		testAdminRepository.initialize();

		testAdminCon = testAdminRepository.getConnection();

		Repository otherrepo = factory.getRepository(adminconfig);
		RepositoryConnection conn = null;
		try {
			// try to get connection without initializing repo, will throw error
			conn = otherrepo.getConnection();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
			Assert.assertTrue(conn == null);
			otherrepo.shutDown();
		}

		graph1 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/Graph1");
		graph2 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/Graph2");
		dirgraph = testAdminCon.getValueFactory().createIRI("http://marklogic.com/dirgraph");
		dirgraph1 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/dirgraph1");

		// Creating MLRDF4J Connection object Using MarkLogicRepository
		// overloaded constructor
		if (testReaderCon == null || testReaderRepository == null) {
			testReaderRepository = new MarkLogicRepository(host, restPort, "reader", "reader", "DIGEST");
			try {
				testReaderRepository.initialize();
				Assert.assertNotNull(testReaderRepository);
				testReaderCon = testReaderRepository.getConnection();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
		
		// Creating MLRDF4J Connection object Using
		// MarkLogicRepository(databaseclient) constructor
		if (databaseClient == null) {
			databaseClient = DatabaseClientFactory.newClient(host, restPort,
					new DatabaseClientFactory.DigestAuthContext("writer", "writer"));
		}

		if (testWriterCon == null || testWriterRepository == null) {
			testWriterRepository = new MarkLogicRepository(databaseClient);
			qmgr = databaseClient.newQueryManager();

			try {
				testWriterRepository.initialize();
				testWriterCon = testWriterRepository.getConnection();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testDBClient() throws Exception {
		tearDown();
		
		MarkLogicRepositoryConfig adminconfig = new MarkLogicRepositoryConfig();
		adminconfig.setHost(host);
		adminconfig.setAuth("DIGEST");
		adminconfig.setUser("admin");
		adminconfig.setPassword("admin");
		adminconfig.setPort(restPort);
		MarkLogicRepositoryFactory factory = new MarkLogicRepositoryFactory();
		Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
		try {
			testAdminRepository = factory.getRepository(adminconfig);
			testAdminCon = testAdminRepository.getConnection();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		}
		try {
			testAdminRepository.initialize();
			testAdminCon = testAdminRepository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		DatabaseClient client = testAdminCon.getDatabaseClient();
		client.newDocumentManager().writeAs("/test1",new StringHandle().with("<test>test1</test>").withFormat(Format.XML));
		Assert.assertTrue(client.newServerEval().xquery("fn:doc(\"/test1\")").eval().hasNext());
		// Creating testAdminCon with MarkLogicRepositoryConfig constructor
		testAdminCon.close();
		testAdminRepository.shutDown();
		testAdminRepository = null;
		testAdminCon = null;

		adminconfig = new MarkLogicRepositoryConfig(host, restPort, "admin", "admin", "DIGEST");
		Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
		testAdminRepository = factory.getRepository(adminconfig);
		testAdminRepository.initialize();

		testAdminCon = testAdminRepository.getConnection();
		client = testAdminCon.getDatabaseClient();
		client.newDocumentManager().writeAs("/test2",new StringHandle().with("<test>test2</test>").withFormat(Format.XML));
		Assert.assertTrue(client.newServerEval().xquery("fn:doc(\"/test2\")").eval().hasNext());

		Repository otherrepo = factory.getRepository(adminconfig);
		RepositoryConnection conn = null;
		try {
			// try to get connection without initializing repo, will throw error
			conn = otherrepo.getConnection();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
			Assert.assertTrue(conn == null);
			otherrepo.shutDown();
		}

		graph1 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/Graph1");
		graph2 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/Graph2");
		dirgraph = testAdminCon.getValueFactory().createIRI("http://marklogic.com/dirgraph");
		dirgraph1 = testAdminCon.getValueFactory().createIRI("http://marklogic.com/dirgraph1");

		// Creating MLRDF4J Connection object Using MarkLogicRepository
		// overloaded constructor
		if (testReaderCon == null || testReaderRepository == null) {
			testReaderRepository = new MarkLogicRepository(host, restPort, "reader", "reader", "DIGEST");
			try {
				testReaderRepository.initialize();
				Assert.assertNotNull(testReaderRepository);
				testReaderCon = testReaderRepository.getConnection();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
		
		client = testReaderCon.getDatabaseClient();
		try {
			client.newDocumentManager().writeAs("/test3",new StringHandle().with("<test>test3</test>").withFormat(Format.XML));
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ForbiddenUserException);
		}
		
				
		// Creating MLRDF4J Connection object Using
		// MarkLogicRepository(databaseclient) constructor
		if (databaseClient == null) {
			databaseClient = DatabaseClientFactory.newClient(host, restPort,
					new DatabaseClientFactory.DigestAuthContext("writer", "writer"));
		}

		if (testWriterCon == null || testWriterRepository == null) {
			testWriterRepository = new MarkLogicRepository(databaseClient);
			qmgr = databaseClient.newQueryManager();

			try {
				testWriterRepository.initialize();
				Assert.assertNotNull(testWriterRepository);
				testWriterCon = testWriterRepository.getConnection();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
		client = testWriterCon.getDatabaseClient();
		DatabaseClient adminClient = testAdminCon.getDatabaseClient();
		client.newDocumentManager().writeAs("/test4",new StringHandle().with("<test>test4</test>").withFormat(Format.XML));
		try {
			client.newServerEval().xquery("fn:doc(\"/test4\")").eval();
			Assert.assertTrue(false);
		}
		catch(Exception e) {
			Assert.assertTrue(e instanceof FailedRequestException);
		}
		Assert.assertTrue(adminClient.newServerEval().xquery("fn:doc(\"/test4\")").eval().hasNext());
		clearDB(restPort);
	}
	
	
	@Ignore
	public void testMultiThreadedAdd() throws Exception {

		class MyRunnable implements Runnable {
			@Override
			public void run() {
				try {
					testAdminCon.begin();
					for (int j = 0; j < 100; j++) {
						IRI subject = vf.createIRI(NS + ID + "/" + Thread.currentThread().getId() + "/" + j + "#1111");
						IRI predicate = fname = vf
								.createIRI(NS + ADDRESS + "/" + Thread.currentThread().getId() + "/" + "#firstName");
						Literal object = vf.createLiteral(Thread.currentThread().getId() + "-" + j + "-" + "John");
						testAdminCon.add(subject, predicate, object, dirgraph);
					}
					testAdminCon.commit();

				} catch (RepositoryException e1) {
					e1.printStackTrace();
				} finally {
					try {
						if (testAdminCon.isActive()) {
							testAdminCon.rollback();
						}
					} catch (UnknownTransactionStateException e) {
						e.printStackTrace();
					} catch (RepositoryException e) {
						e.printStackTrace();
					}
				}
			}

		}

		class MyRunnable1 implements Runnable {

			@Override
			public void run() {
				try {
					testWriterCon.begin();
					for (int j = 0; j < 100; j++) {
						IRI subject = vf.createIRI(NS + ID + "/" + Thread.currentThread().getId() + "/" + j + "#1111");
						IRI predicate = fname = vf
								.createIRI(NS + ADDRESS + "/" + Thread.currentThread().getId() + "/" + "#firstName");
						Literal object = vf.createLiteral(Thread.currentThread().getId() + "-" + j + "-" + "John");
						testWriterCon.add(subject, predicate, object, dirgraph);
					}
					testWriterCon.commit();

				} catch (RepositoryException e1) {
					e1.printStackTrace();
				} finally {
					try {
						if (testWriterCon.isActive()) {
							testWriterCon.rollback();
						}
					} catch (UnknownTransactionStateException e) {
						e.printStackTrace();
					} catch (RepositoryException e) {
						e.printStackTrace();
					}
				}
			}
		}
		Thread t1, t2;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");
		t2 = new Thread(new MyRunnable1());
		t2.setName("T2");

		t1.start();
		t2.start();

		t1.join();
		t2.join();
		Assert.assertEquals(200, testAdminCon.size());
	}

	@Ignore
	public void testMultiThreadedAdd1() throws Exception {

		class MyRunnable implements Runnable {
			@Override
			public void run() {
				MarkLogicRepositoryConnection tempConn = null;
				try {
					tempConn = testAdminRepository.getConnection();
					tempConn.begin();
					for (int j = 0; j < 100; j++) {
						IRI subject = vf.createIRI(NS + ID + "/" + Thread.currentThread().getId() + "/" + j + "#1111");
						IRI predicate = fname = vf
								.createIRI(NS + ADDRESS + "/" + Thread.currentThread().getId() + "/" + "#firstName");
						Literal object = vf.createLiteral(Thread.currentThread().getId() + "-" + j + "-" + "John");
						tempConn.add(subject, predicate, object, dirgraph);
					}
					tempConn.commit();

				} catch (RepositoryException e1) {
					e1.printStackTrace();
				} finally {
					try {
						if (tempConn.isActive()) {
							tempConn.rollback();
						}
					} catch (UnknownTransactionStateException e) {
						e.printStackTrace();
					} catch (RepositoryException e) {
						e.printStackTrace();
					}
				}
			}

		}

		Thread t1, t2, t3, t4;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");
		t2 = new Thread(new MyRunnable());
		t2.setName("T2");
		t3 = new Thread(new MyRunnable());
		t3.setName("T3");
		t4 = new Thread(new MyRunnable());
		t4.setName("T4");

		t1.start();
		t2.start();
		t3.start();
		t4.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();

		Assert.assertEquals(400, testAdminCon.size());
	}

	@Test
	public void testMultiThreadedAdd2() throws Exception {

		Assert.assertEquals(0, testAdminCon.size());
		class MyRunnable implements Runnable {
			@Override
			public void run() {
				try {
					for (int j = 0; j < 100; j++) {
						IRI subject = vf.createIRI(NS + ID + "/" + Thread.currentThread().getId() + "/" + j + "#1111");
						IRI predicate = vf
								.createIRI(NS + ADDRESS + "/" + Thread.currentThread().getId() + "/" + "#firstName");
						Literal object = vf.createLiteral(Thread.currentThread().getId() + "-" + j + "-" + "John");
						testAdminCon.add(subject, predicate, object, dirgraph);
					}
				} catch (RepositoryException e1) {
					e1.printStackTrace();
				}
			}
		}

		Thread t1, t2, t3, t4;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");
		t2 = new Thread(new MyRunnable());
		t2.setName("T2");
		t3 = new Thread(new MyRunnable());
		t3.setName("T3");
		t4 = new Thread(new MyRunnable());
		t4.setName("T4");

		t1.start();
		t2.start();
		t3.start();
		t4.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();

		Assert.assertEquals(400, testAdminCon.size());
	}

	@Test
	public void testMultiThreadedAddDuplicate() throws Exception {

		class MyRunnable implements Runnable {

			@Override
			public void run() {

				for (int j = 0; j < 100; j++) {
					IRI subject = vf.createIRI(NS + ID + "/" + j + "#1111");
					IRI predicate = fname = vf.createIRI(NS + ADDRESS + "/" + "#firstName");
					Literal object = vf.createLiteral(j + "-" + "John");

					try {
						testAdminCon.add(subject, predicate, object, dirgraph);
					} catch (RepositoryException e) {
						e.printStackTrace();
					}

				}
			}
		}
		Thread t1, t2, t3, t4, t5;
		t1 = new Thread(new MyRunnable());
		t2 = new Thread(new MyRunnable());
		t3 = new Thread(new MyRunnable());
		t4 = new Thread(new MyRunnable());
		t5 = new Thread(new MyRunnable());

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Assert.assertEquals(100, testAdminCon.size());

	}

	// ISSUE - 19
	@Test
	public void testPrepareBooleanQuery1() throws Exception {
		Assert.assertEquals(0L, testAdminCon.size());
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "tigers.ttl");
		testAdminCon.add(in, "", RDFFormat.TURTLE);
		in.close();
		Assert.assertEquals(107L, testAdminCon.size());

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>" + " ASK " + " WHERE" + " {"
				+ " ?id bb:lastname  ?name ." + " FILTER  EXISTS { ?id bb:country ?countryname }" + " }";

		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1).evaluate();
		Assert.assertFalse(result1);

		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"
				+ "PREFIX  r: <http://marklogic.com/baseball/rules#>" + " ASK WHERE" + " {" + " ?id bb:team r:Tigers."
				+ " ?id bb:position \"pitcher\"." + " }";

		boolean result2 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query2).evaluate();
		Assert.assertTrue(result2);
	}

	// ISSUE 32, 45
	@Test
	public void testPrepareBooleanQuery2() throws Exception {

		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "tigers.ttl");
		Reader reader = new InputStreamReader(in);
		testAdminCon.add(reader, "http://marklogic.com/baseball/", RDFFormat.TURTLE, graph1);
		reader.close();

		Assert.assertEquals(107L, testAdminCon.size(graph1, null));

		String query1 = "ASK FROM <http://marklogic.com/Graph1>" + " WHERE" + " {" + " ?player ?team <#Tigers>." + " }";

		boolean result1 = testAdminCon
				.prepareBooleanQuery(QueryLanguage.SPARQL, query1, "http://marklogic.com/baseball/rules").evaluate();
		Assert.assertTrue(result1);

		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"
				+ " PREFIX  r: <http://marklogic.com/baseball/rules#>"
				+ " ASK  FROM <http://marklogic.com/Graph1> WHERE" + " {" + " ?id bb:team r:Tigers."
				+ " ?id bb:position \"pitcher\"." + " }";

		boolean result2 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query2, "").evaluate();
		Assert.assertTrue(result2);

	}

	@Test
	public void testPrepareBooleanQuery3() throws Exception {

		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl");

		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);

		Assert.assertEquals(107L, testAdminCon.size());

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>" + "ASK " + "WHERE" + "{"
				+ " ?s bb:position ?o." + "}";

		BooleanQuery bq = testAdminCon.prepareBooleanQuery(query1);
		bq.setBinding("o", vf.createLiteral("coach"));
		boolean result1 = bq.evaluate();
		Assert.assertTrue(result1);
		bq.clearBindings();

		bq.setBinding("o", vf.createLiteral("pitcher"));
		boolean result2 = bq.evaluate();
		Assert.assertTrue(result2);
		bq.clearBindings();

		bq.setBinding("o", vf.createLiteral("abcd"));
		boolean result3 = bq.evaluate();
		Assert.assertFalse(result3);
	}

	@Test
	public void testPrepareBooleanQuery4() throws Exception {

		File file = new File(
				MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl").getFile());
		testAdminCon.add(file, "", RDFFormat.TURTLE, graph1);
		logger.debug(file.getAbsolutePath());

		Assert.assertEquals(107L, testAdminCon.size(graph1));

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"
				+ "ASK FROM <http://marklogic.com/Graph1>" + "WHERE" + "{" + "<#119> <#lastname> \"Verlander\"."
				+ "<#119> <#team> ?tigers." + "}";

		boolean result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
	}

	// ISSUE 20 , 25
	@Test
	public void testPrepareTupleQuery1() throws Exception {

		Assert.assertEquals(0, testAdminCon.size());

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testAdminCon.add(st1, dirgraph);
		testAdminCon.add(st2, dirgraph);
		testAdminCon.add(st3, dirgraph);
		testAdminCon.add(st4, dirgraph);
		testAdminCon.add(st5, dirgraph);
		testAdminCon.add(st6, dirgraph);
		testAdminCon.add(st7, dirgraph);
		testAdminCon.add(st8, dirgraph);
		testAdminCon.add(st9, dirgraph);
		testAdminCon.add(st10, dirgraph);

		Assert.assertEquals(10, testAdminCon.size(dirgraph));

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append("         SELECT DISTINCT ?person");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person ad:firstName ?firstname ;");
		queryBuilder.append(" ad:lastName ?lastname.");
		queryBuilder.append(" OPTIONAL {?person ad:homeTel ?phonenumber .}");
		queryBuilder.append(" FILTER (?firstname = \"Fei\")}");

		TupleQuery query = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		TupleQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				Value nameResult = solution.getValue("person");
				Assert.assertEquals(nameResult.stringValue(), fei.stringValue());
			}
		} finally {
			result.close();
		}

	}

	@Test
	public void testPrepareTupleQuery2() throws Exception {

		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
		testAdminCon.add(john, email, johnemail, dirgraph);

		testAdminCon.add(micah, fname, micahfname, dirgraph);
		testAdminCon.add(micah, lname, micahlname, dirgraph);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph);

		testAdminCon.add(fei, fname, feifname, dirgraph);
		testAdminCon.add(fei, lname, feilname, dirgraph);
		testAdminCon.add(fei, email, feiemail, dirgraph);

		try {
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder();

		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append("         SELECT ?person ?lastname");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person <#firstName> ?firstname ;");
		queryBuilder.append(" <#lastName> ?lastname.");
		queryBuilder.append(" OPTIONAL {?person <#email> ?email.}");
		queryBuilder.append("  FILTER  EXISTS  {?person <#homeTel> ?tel .}} ORDER BY ?lastname");

		TupleQuery query = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString(),
				"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();

		String[] expectedPersonresult = { micah.stringValue(), john.stringValue() };
		String[] expectedLnameresult = { micahlname.stringValue(), johnlname.stringValue() };
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();

				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));

				Value personResult = solution.getValue("person");
				Value nameResult = solution.getValue("lastname");

				Assert.assertEquals(personResult.stringValue(), expectedPersonresult[i]);
				Assert.assertEquals(nameResult.stringValue(), expectedLnameresult[i]);
				i++;
			}
		} finally {
			result.close();
		}

	}

	@Test
	public void testPrepareTupleQuery3() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);
		Statement st4 = vf.createStatement(john, email, johnemail);
		Statement st5 = vf.createStatement(micah, fname, micahfname);
		Statement st6 = vf.createStatement(micah, lname, micahlname);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel);
		Statement st8 = vf.createStatement(fei, fname, feifname);
		Statement st9 = vf.createStatement(fei, lname, feilname);
		Statement st10 = vf.createStatement(fei, email, feiemail);

		testAdminCon.add(st1, dirgraph);
		testAdminCon.add(st2, dirgraph);
		testAdminCon.add(st3, dirgraph);
		testAdminCon.add(st4, dirgraph);
		testAdminCon.add(st5, dirgraph);
		testAdminCon.add(st6, dirgraph);
		testAdminCon.add(st7, dirgraph);
		testAdminCon.add(st8, dirgraph);
		testAdminCon.add(st9, dirgraph);
		testAdminCon.add(st10, dirgraph);

		try {
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder();

		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?name ?id ?g ");
		queryBuilder.append(" FROM NAMED  ");
		queryBuilder.append("<").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE ");
		queryBuilder.append("  {  ");
		queryBuilder.append(" GRAPH ?g { ?id ad:lastName  ?name .} ");
		queryBuilder.append(" FILTER  EXISTS { GRAPH ?g  {?id ad:email ?email ;  ");
		queryBuilder.append("  ad:firstName ?fname.}");
		queryBuilder.append("  } ");
		queryBuilder.append(" }  ");
		queryBuilder.append(" ORDER BY ?name ");

		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString());
		TupleQueryResult result = query.evaluate();

		String[] epectedPersonresult = { fei.stringValue(), john.stringValue() };
		String[] expectedLnameresult = { feilname.stringValue(), johnlname.stringValue() };
		String[] expectedGraphresult = { dirgraph.stringValue(), dirgraph.stringValue() };

		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("name"), is(equalTo(true)));
				assertThat(solution.hasBinding("id"), is(equalTo(true)));
				assertThat(solution.hasBinding("g"), is(equalTo(true)));

				Value idResult = solution.getValue("id");
				Value nameResult = solution.getValue("name");
				Value graphResult = solution.getValue("g");

				Assert.assertEquals(idResult.stringValue(), epectedPersonresult[i]);
				Assert.assertEquals(nameResult.stringValue(), expectedLnameresult[i]);
				Assert.assertEquals(graphResult.stringValue(), expectedGraphresult[i]);
				i++;
			}
		} finally {
			result.close();
		}

	}

	// ISSSUE 109
	@Test
	public void testPrepareTupleQuery4() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(st3);
		testAdminCon.add(st4);
		testAdminCon.add(st5);
		testAdminCon.add(st6);
		testAdminCon.add(st7);
		testAdminCon.add(st8);
		testAdminCon.add(st9);
		testAdminCon.add(st10);

		try {
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder(64);

		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?person ?firstname ?lastname ?phonenumber");
		queryBuilder.append(" FROM <").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person <#firstName> ?firstname ;");
		queryBuilder.append("           <#lastName> ?lastname. ");
		queryBuilder.append("   OPTIONAL {?person <#homeTel> ?phonenumber .} ");
		queryBuilder.append("   VALUES ?firstname { \"Micah\" \"Fei\" }");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?firstname");

		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),
				"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();

		String[] epectedPersonresult = { "http://marklogicsparql.com/id#3333", "http://marklogicsparql.com/id#2222" };
		String[] expectedLnameresult = { "Ling", "Dubinko" };
		String[] expectedFnameresult = { "Fei", "Micah" };
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				Value personResult = solution.getValue("person");
				Value lnameResult = solution.getValue("lastname");
				Value fnameResult = solution.getValue("firstname");
				Literal phoneResult = (Literal) solution.getValue("phonenumber");
				Assert.assertEquals(epectedPersonresult[i], personResult.stringValue());
				Assert.assertEquals(expectedLnameresult[i], lnameResult.stringValue());
				Assert.assertEquals(expectedFnameresult[i], fnameResult.stringValue());
				try {
					assertThat(phoneResult.doubleValue(), is(equalTo(new Double(22222222D))));
				} catch (NullPointerException e) {

				}
				i++;
			}
		} finally {
			result.close();
		}

	}

	// ISSUE 197
	@Test
	public void testPrepareTupleQuerywithBidings() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(st3);
		testAdminCon.add(st4);
		testAdminCon.add(st5);
		testAdminCon.add(st6);
		testAdminCon.add(st7);
		testAdminCon.add(st8);
		testAdminCon.add(st9);
		testAdminCon.add(st10);

		try {
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder(64);

		queryBuilder.append(" SELECT ?person ?firstname ?lastname ?phonenumber");
		queryBuilder.append(" FROM <").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person <#firstName> ?firstname ;");
		queryBuilder.append("           <#lastName> ?lastname ; ");
		queryBuilder.append("           <#homeTel> ?phonenumber .} ");
		queryBuilder.append(" ORDER BY ?lastname");

		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),
				"http://marklogicsparql.com/addressbook");
		query.setBinding("firstname", vf.createLiteral("Micah"));

		TupleQueryResult result = query.evaluate();

		String[] epectedPersonresult = { "http://marklogicsparql.com/id#2222" };
		String[] expectedLnameresult = { "Dubinko" };
		String[] expectedFnameresult = { "Micah" };
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				Value personResult = solution.getValue("person");
				Value lnameResult = solution.getValue("lastname");
				Value fnameResult = solution.getValue("firstname");
				Literal phoneResult = (Literal) solution.getValue("phonenumber");
				Assert.assertEquals(epectedPersonresult[i], personResult.stringValue());
				Assert.assertEquals(expectedLnameresult[i], lnameResult.stringValue());
				Assert.assertEquals(expectedFnameresult[i], fnameResult.stringValue());
				assertThat(phoneResult.doubleValue(), is(equalTo(new Double(22222222D))));
				i++;
			}
		} finally {
			result.close();
		}

	}

	@Test
	public void testPrepareTupleQueryEmptyResult() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st3 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st4 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);

		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(st3);
		testAdminCon.add(st4);

		try {
			Assert.assertEquals(4, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder(64);

		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?person ?p ?o");
		queryBuilder.append(" FROM <").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person <#firstName> ?firstname ;");
		queryBuilder.append("           <#lastName> ?lastname. ");
		queryBuilder.append("   OPTIONAL {?person <#homeTel> ?phonenumber .} ");
		queryBuilder.append("   FILTER NOT EXISTS {?person ?p ?o .}");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?person");

		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),
				"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();

		assertThat(result, is(notNullValue()));
		Assert.assertFalse(result.hasNext());
	}

	// ISSUE 230
	@Test
	public void testPrepareGraphQuery1() throws Exception {
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" CONSTRUCT{ ?person ?p ?o .} ");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE ");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person ad:firstName ?firstname ; ");
		queryBuilder.append("           ad:lastName  ?lastname ;  ");
		queryBuilder.append("           ?p ?o . ");
		queryBuilder.append(" } ");
		queryBuilder.append(" order by $person ?p ?o ");

		GraphQuery emptyQuery = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		emptyQuery.setBinding("firstname", vf.createLiteral("Micah"));
		GraphQueryResult emptyResult = null;
		try {
			emptyResult = emptyQuery.evaluate();
			assertFalse(emptyResult == null);
			assertThat(emptyResult.hasNext(), is(equalTo(false)));
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} finally {
			emptyResult.close();
		}
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		sL.add(st4);
		sL.add(st5);
		sL.add(st6);
		sL.add(st7);
		sL.add(st8);
		sL.add(st9);
		sL.add(st10);

		StatementIterator iter = new StatementIterator(sL);
		testAdminCon.add(new StatementIterable(iter), dirgraph);
		Assert.assertEquals(10, testAdminCon.size(dirgraph));
		GraphQuery query = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("firstname", vf.createLiteral("Micah"));

		GraphQueryResult result = query.evaluate();

		Literal[] expectedObjectresult = { micahfname, micahhomeTel, micahlname };
		IRI[] expectedPredicateresult = { fname, homeTel, lname };
		int i = 0;

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				IRI subject = (IRI) st.getSubject();
				Assert.assertEquals(subject, micah);
				IRI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		} finally {
			result.close();
		}
		StringBuilder qB = new StringBuilder(128);
		qB.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		qB.append(" CONSTRUCT{ ?person ?p ?o .} ");
		qB.append(" FROM <http://marklogic.com/dirgraph>");
		qB.append(" WHERE ");
		qB.append(" { ");
		qB.append("   ?person ad:firstname ?firstname ; ");
		qB.append("  ?p ?o . ");
		qB.append("  VALUES ?firstname { \"Fei\" }  ");
		qB.append(" } ");
		qB.append(" order by $person ?p ?o ");

		GraphQuery query1 = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, qB.toString());
		GraphQueryResult result1 = null;
		try {
			result1 = query1.evaluate();
			assertThat(result1, is(notNullValue()));
			Assert.assertFalse(result1.hasNext());
		} finally {
			result1.close();
		}
	}

	// ISSUE 45
	@Test
	public void testPrepareGraphQuery2() throws Exception {
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		sL.add(st4);
		sL.add(st5);
		sL.add(st6);
		sL.add(st7);
		sL.add(st8);
		sL.add(st9);
		sL.add(st10);

		StatementIterator iter = new StatementIterator(sL);
		Iteration<Statement, Exception> it = new IteratorIteration<>(iter);
		testAdminCon.add(it, dirgraph);
		Assert.assertEquals(10, testAdminCon.size(dirgraph));

		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX id:  <http://marklogicsparql.com/id#> ");
		queryBuilder.append(" CONSTRUCT{ <#1111> ad:email ?e .} ");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph> ");
		queryBuilder.append(" WHERE ");
		queryBuilder.append(" { ");
		queryBuilder.append("  <#1111> ad:lastName ?o; ");
		queryBuilder.append("          ad:email  ?e. ");
		queryBuilder.append(" }  ");

		GraphQuery query = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString(),
				"http://marklogicsparql.com/id");
		GraphQueryResult result = query.evaluate();

		Literal[] expectedObjectresult = { johnemail };
		IRI[] expectedPredicateresult = { email };
		int i = 0;

		try {
			assertThat(result, is(notNullValue()));
			while (result.hasNext()) {
				Statement st = result.next();
				IRI subject = (IRI) st.getSubject();
				Assert.assertEquals(subject, john);
				IRI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		} finally {
			result.close();
		}

	}

	// ISSUE 44, 53, 138, 153, 257
	@Test
	public void testPrepareGraphQuery3() throws Exception {
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testWriterCon.add(st1);
		testWriterCon.add(st2);
		testWriterCon.add(st3);
		testWriterCon.add(st4);
		testWriterCon.add(st5);
		testWriterCon.add(st6);
		testWriterCon.add(st7);
		testWriterCon.add(st8);
		testWriterCon.add(st9);
		testWriterCon.add(st10);

		Assert.assertTrue(testWriterCon.hasStatement(st1, false));
		Assert.assertFalse(testWriterCon.hasStatement(st1, false, (Resource) null));
		Assert.assertFalse(testWriterCon.hasStatement(st1, false, new Resource[] { null }));
		Assert.assertTrue(testWriterCon.hasStatement(st1, false, dirgraph));

		Assert.assertEquals(10, testAdminCon.size(dirgraph));

		String query = " DESCRIBE <http://marklogicsparql.com/addressbook#firstName> ";
		GraphQuery queryObj = testReaderCon.prepareGraphQuery(query);

		GraphQueryResult result = queryObj.evaluate();
		result.hasNext();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(false)));
		} finally {
			result.close();
		}
	}

	// ISSUE 46
	@Test
	public void testPrepareGraphQuery4() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);
		Statement st4 = vf.createStatement(john, email, johnemail);
		Statement st5 = vf.createStatement(micah, fname, micahfname);
		Statement st6 = vf.createStatement(micah, lname, micahlname);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel);
		Statement st8 = vf.createStatement(fei, fname, feifname);
		Statement st9 = vf.createStatement(fei, lname, feilname);
		Statement st10 = vf.createStatement(fei, email, feiemail);

		testWriterCon.add(st1, dirgraph);
		testWriterCon.add(st2, dirgraph);
		testWriterCon.add(st3, dirgraph);
		testWriterCon.add(st4, dirgraph);
		testWriterCon.add(st5, dirgraph);
		testWriterCon.add(st6, dirgraph);
		testWriterCon.add(st7, dirgraph);
		testWriterCon.add(st8, dirgraph);
		testWriterCon.add(st9, dirgraph);
		testWriterCon.add(st10, dirgraph);

		Assert.assertEquals(10, testWriterCon.size(dirgraph));

		String query = " DESCRIBE  <#3333>  ";
		GraphQuery queryObj = testReaderCon.prepareGraphQuery(query, "http://marklogicsparql.com/id");

		GraphQueryResult result = queryObj.evaluate();
		int i = 0;

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				IRI subject = (IRI) st.getSubject();
				Assert.assertNotNull(subject);
				IRI predicate = st.getPredicate();
				Assert.assertNotNull(predicate);
				Value object = st.getObject();
				Assert.assertNotNull(object);
				i++;
			}
		} finally {
			result.close();
		}
		Assert.assertEquals(3, i);

	}

	// ISSUE 70
	@Test
	public void testPrepareQuery1() throws Exception {
		testAdminCon.add(
				MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "companies_100.ttl"), "",
				RDFFormat.TURTLE, (Resource) null);
		Assert.assertEquals(testAdminCon.size(), 1600L);

		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append("PREFIX demor: <http://demo/resource#>");
		queryBuilder.append(" PREFIX demov: <http://demo/verb#>");
		queryBuilder.append(" PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>");
		queryBuilder.append(" SELECT (COUNT(?company) AS ?total)");
		queryBuilder.append(" WHERE { ");
		queryBuilder.append("  ?company a vcard:Organization .");
		queryBuilder.append("  ?company demov:industry ?industry .");
		queryBuilder.append("  ?company vcard:hasAddress/vcard:postal-code ?zip .");
		queryBuilder.append("  ?company vcard:hasAddress/vcard:postal-code ?whatcode ");
		queryBuilder.append(" } ");

		Query query = testAdminCon.prepareQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("whatcode", vf.createLiteral("33333"));
		TupleQueryResult result = null;
		if (query instanceof TupleQuery) {
			result = ((TupleQuery) query).evaluate();

		}

		try {
			assertThat(result, is(notNullValue()));

			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("total"), is(equalTo(true)));
				Value totalResult = solution.getValue("total");
				Assert.assertEquals(vf.createLiteral("12", XMLSchema.UNSIGNED_LONG), totalResult);

			}
		} finally {
			result.close();
		}
	}

	// ISSUE 70
	@Test
	public void testPrepareQuery2() throws Exception {

		Reader ir = new BufferedReader(new InputStreamReader(
				MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "property-paths.ttl")));
		testAdminCon.add(ir, "", RDFFormat.TURTLE, (Resource) null);

		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" prefix : <http://learningsparql.com/ns/papers#> ");
		queryBuilder.append(" prefix c: <http://learningsparql.com/ns/citations#>");
		queryBuilder.append(" SELECT ?s");
		queryBuilder.append(" WHERE {  ");
		queryBuilder.append(" ?s ^c:cites :paperK2 . ");
		queryBuilder.append(" FILTER (?s != :paperK2)");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?s ");

		Query query = testAdminCon.prepareQuery(queryBuilder.toString());
		query.setBinding("whatcode", vf.createLiteral("33333"));
		TupleQueryResult result = null;
		if (query instanceof TupleQuery) {
			result = ((TupleQuery) query).evaluate();

		}

		try {
			assertThat(result, is(notNullValue()));

			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value totalResult = solution.getValue("s");
				Assert.assertEquals(vf.createIRI("http://learningsparql.com/ns/papers#paperJ"), totalResult);

			}
		} finally {
			result.close();
		}
	}

	@Test
	public void testPrepareQuery3() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);

		testWriterCon.add(st1, dirgraph);
		testWriterCon.add(st2, dirgraph);
		testWriterCon.add(st3, dirgraph);

		Assert.assertEquals(3, testWriterCon.size(dirgraph));

		String query = " DESCRIBE  <http://marklogicsparql.com/id#1111>  ";
		Query queryObj = testReaderCon.prepareQuery(query, "http://marklogicsparql.com/id");
		GraphQueryResult result = null;

		if (queryObj instanceof GraphQuery) {
			result = ((GraphQuery) queryObj).evaluate();

		}

		Literal[] expectedObjectresult = { johnfname, johnlname, johnhomeTel };
		IRI[] expectedPredicateresult = { fname, lname, homeTel };
		int i = 0;

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				IRI subject = (IRI) st.getSubject();
				Assert.assertEquals(subject, john);
				IRI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		} finally {
			result.close();
		}
	}

	// ISSUE 70
	@Test
	public void testPrepareQuery4() throws Exception {

		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE);
		Assert.assertEquals(107L, testAdminCon.size());

		String query1 = "ASK " + "WHERE" + "{" + " ?s <#position> ?o." + "}";

		Query bq = testAdminCon.prepareQuery(query1, "http://marklogic.com/baseball/players");
		bq.setBinding("o", vf.createLiteral("pitcher"));
		boolean result1 = ((BooleanQuery) bq).evaluate();
		Assert.assertTrue(result1);

	}

	// Bug 35241
	@Ignore
	public void testPrepareMultipleBaseIRI1() throws Exception {

		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
		testAdminCon.add(john, email, johnemail, dirgraph);

		testAdminCon.add(micah, fname, micahfname, dirgraph);
		testAdminCon.add(micah, lname, micahlname, dirgraph);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph);

		testAdminCon.add(fei, fname, feifname, dirgraph);
		testAdminCon.add(fei, lname, feilname, dirgraph);
		testAdminCon.add(fei, email, feiemail, dirgraph);

		try {
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		} catch (Exception ex) {
			logger.error("Failed :", ex);
		}

		StringBuilder queryBuilder = new StringBuilder();

		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append(" BASE <http://marklogicsparql.com/addressbook>");
		queryBuilder.append(" BASE <http://marklogicsparql.com/id>");
		queryBuilder.append("         SELECT ?person ?lastname");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person <#firstName> ?firstname ;");
		queryBuilder.append(" <#lastName> ?lastname.");
		queryBuilder.append(" OPTIONAL {<#1111> <#email> ?email.}");
		queryBuilder.append("  FILTER  EXISTS  {?person <#homeTel> ?tel .}} ORDER BY ?lastname");

		TupleQuery query = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		TupleQueryResult result = query.evaluate();

		String[] expectedPersonresult = { micah.stringValue(), john.stringValue() };
		String[] expectedLnameresult = { micahlname.stringValue(), johnlname.stringValue() };
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();

				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));

				Value personResult = solution.getValue("person");
				Value nameResult = solution.getValue("lastname");

				Assert.assertEquals(personResult.stringValue(), expectedPersonresult[i]);
				Assert.assertEquals(nameResult.stringValue(), expectedLnameresult[i]);
				i++;
			}
		} finally {
			result.close();
		}
	}

	// ISSUE 106, 133, 183
	@Test
	public void testCommit() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(john, email, johnemail, dirgraph);

			Assert.assertTrue("Uncommitted update should be visible to own connection",
					testAdminCon.hasStatement(john, email, johnemail, false, dirgraph));
			Assert.assertFalse("Uncommitted update should only be visible to own connection",
					testReaderCon.hasStatement(john, email, johnemail, false, dirgraph));
			Assert.assertEquals(testWriterCon.size(), 0L);

			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		Assert.assertEquals(testWriterCon.size(), 1L);
		Assert.assertTrue("Repository should contain statement after commit",
				testAdminCon.hasStatement(john, email, johnemail, false, dirgraph));
		Assert.assertTrue("Committed update will be visible to all connection",
				testReaderCon.hasStatement(john, email, johnemail, false, dirgraph));
	}

	// ISSUE 183
	@Test
	public void testSizeRollback() throws Exception {
		testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT);
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		try {
			testAdminCon.begin();
			testAdminCon.add(john, fname, johnfname, dirgraph);
			assertThat(testAdminCon.size(), is(equalTo(1L)));
			assertThat(testWriterCon.size(), is(equalTo(0L)));
			testAdminCon.add(john, fname, feifname);
			assertThat(testAdminCon.size(), is(equalTo(2L)));
			assertThat(testWriterCon.size(), is(equalTo(0L)));
			testAdminCon.rollback();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
	}

	// ISSUE 133, 183
	@Test
	public void testSizeCommit() throws Exception {
		testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT);
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		try {
			testAdminCon.begin();
			testAdminCon.add(john, fname, johnfname, dirgraph);
			assertThat(testAdminCon.size(), is(equalTo(1L)));
			assertThat(testWriterCon.size(), is(equalTo(0L)));
			testAdminCon.add(john, fname, feifname);
			assertThat(testAdminCon.size(), is(equalTo(2L)));
			assertThat(testWriterCon.size(), is(equalTo(0L)));
			testAdminCon.commit();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();

		}
		assertThat(testAdminCon.size(), is(equalTo(2L)));
		assertThat(testWriterCon.size(), is(equalTo(2L)));
	}

	// ISSUE 121, 174
	@Test
	public void testTransaction() throws Exception {

		testAdminCon.begin();
		testAdminCon.commit();
		try {
			testAdminCon.commit();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof MarkLogicTransactionException);
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		try {
			testAdminCon.rollback();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e2) {
			Assert.assertTrue(e2 instanceof MarkLogicTransactionException);
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		try {
			testAdminCon.begin();
			testAdminCon.prepareUpdate(QueryLanguage.SPARQL,
					"DELETE DATA {GRAPH <" + dirgraph.stringValue() + "> { <" + micah.stringValue() + "> <"
							+ homeTel.stringValue() + "> \"" + micahhomeTel.doubleValue()
							+ "\"^^<http://www.w3.org/2001/XMLSchema#double>} }")
					.execute();
			testAdminCon.commit();
			Assert.assertTrue(testAdminCon.size() == 0);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof MarkLogicTransactionException);
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		try {
			testAdminCon.begin();
			testAdminCon.begin();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof MarkLogicTransactionException);
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

	}

	// ISSUE 123, 122, 175, 185
	@Test
	public void testGraphPerms1() throws Exception {

		GraphManager gmgr = databaseClient.newGraphManager();
		createUserRolesWithPrevilages("test-role");
		GraphPermissions gr = testAdminCon.getDefaultGraphPerms();
		MarkLogicUpdateQuery updateQuery = null;
		String defGraphQuery = null;
		// ISSUE # 175 uncomment after issue is fixed
		Assert.assertEquals(0L, gr.size());
		try {
			testAdminCon.setDefaultGraphPerms(gmgr.permission("test-role", Capability.READ, Capability.UPDATE));
			defGraphQuery = "CREATE GRAPH <http://marklogic.com/test/graph/permstest> ";
			updateQuery = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
			updateQuery.execute();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		try {
			String defGraphQuery1 = "INSERT DATA { GRAPH <http://marklogic.com/test/graph/permstest> { <http://marklogic.com/test1> <pp2> \"test\" } }";
			String checkQuery = "ASK WHERE {  GRAPH <http://marklogic.com/test/graph/permstest> {<http://marklogic.com/test> <pp2> \"test\" }}";
			MarkLogicUpdateQuery updateQuery1 = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery1);
			updateQuery1.execute();
			BooleanQuery booleanQuery = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
			boolean results = booleanQuery.evaluate();
			Assert.assertEquals(false, results);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		gr = testAdminCon.getDefaultGraphPerms();
		Assert.assertEquals(1L, gr.size());
		Iterator<Entry<String, Set<Capability>>> resultPerm = gr.entrySet().iterator();
		while (resultPerm.hasNext()) {
			Entry<String, Set<Capability>> perms = resultPerm.next();
			Assert.assertTrue("test-role" == perms.getKey());
			Iterator<Capability> capability = perms.getValue().iterator();
			while (capability.hasNext())
				assertThat(capability.next().toString(), anyOf(equalTo("UPDATE"), is(equalTo("READ"))));
		}

		try {
			String defGraphQuery2 = "CREATE GRAPH <http://marklogic.com/test/graph/permstest1> ";
			testAdminCon.setDefaultGraphPerms(null);
			updateQuery = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery2);
			updateQuery.execute();
			gr = testAdminCon.getDefaultGraphPerms();
			Assert.assertEquals(0L, gr.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		try {
			createUserRolesWithPrevilages("multitest-role");
			testAdminCon.setDefaultGraphPerms(
					gmgr.permission("multitest-role", Capability.READ).permission("test-role", Capability.UPDATE));
			defGraphQuery = "CREATE GRAPH <http://marklogic.com/test/graph/permstest2> ";
			updateQuery = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
			updateQuery.execute();
			gr = testAdminCon.getDefaultGraphPerms();
			Assert.assertEquals(2L, gr.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		testAdminCon.setDefaultGraphPerms(null);
		testAdminCon.setDefaultGraphPerms(null);
		// ISSUE 180
		// testAdminCon.setDefaultGraphPerms((GraphPermissions)null);
		gr = testAdminCon.getDefaultGraphPerms();
		Assert.assertEquals(0L, gr.size());
	}

	// ISSUE 108
	@Test
	public void testAddDelete() throws org.eclipse.rdf4j.RDF4JException {
		final Statement st1 = vf.createStatement(john, fname, johnfname);
		try {
			testWriterCon.begin();
			testWriterCon.add(st1);
			testWriterCon.prepareUpdate(QueryLanguage.SPARQL, "DELETE DATA {<" + john.stringValue() + "> <"
					+ fname.stringValue() + "> \"" + johnfname.stringValue() + "\"}").execute();
			testWriterCon.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (testWriterCon.isActive())
				testWriterCon.rollback();
		}
		testWriterCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				assertThat(st, is(not(equalTo(st1))));
			}
		});
	}

	// ISSUE 108, 250
	@Test
	public final void testInsertRemove() throws org.eclipse.rdf4j.RDF4JException {
		Statement st = null;
		try {
			testAdminCon.begin();
			testAdminCon.prepareUpdate("INSERT DATA {GRAPH <" + dirgraph.stringValue() + "> { <" + john.stringValue()
					+ "> <" + homeTel.stringValue() + "> \"" + johnhomeTel.doubleValue()
					+ "\"^^<http://www.w3.org/2001/XMLSchema#double>}}").execute();

			RepositoryResult<Statement> result = testAdminCon.getStatements(null, null, null, false);

			try {
				assertNotNull("Iterator should not be null", result);
				assertTrue("Iterator should not be empty", result.hasNext());
				Assert.assertEquals("There should be only one statement in repository", 1L, testAdminCon.size());

				while (result.hasNext()) {
					st = result.next();
					assertNotNull("Statement should not be in a context ", st.getContext());
					assertTrue("Statement predicate should be equal to homeTel ", st.getPredicate().equals(homeTel));

				}
			} finally {
				result.close();
			}

			testAdminCon.remove(st, dirgraph);
			testAdminCon.commit();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();

		}
		Assert.assertEquals(0L, testAdminCon.size());
		testAdminCon.exportStatements(null, null, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st1) throws RDFHandlerException {
				assertThat(st1, is((equalTo(null))));
			}
		}, dirgraph);
	}

	// ISSUE 108, 45
	@Test
	public void testInsertDeleteInsertWhere() throws Exception {
		Assert.assertEquals(0L, testAdminCon.size());

		final Statement st1 = vf.createStatement(john, email, johnemail, dirgraph);
		final Statement st2 = vf.createStatement(john, lname, johnlname);
		testAdminCon.add(st1);
		testAdminCon.add(st2, dirgraph);
		Assert.assertEquals(2L, testAdminCon.size());
		try {
			testAdminCon.begin();
			testAdminCon.prepareUpdate(QueryLanguage.SPARQL, "INSERT DATA {GRAPH <" + dirgraph.stringValue() + "> { <"
					+ john.stringValue() + "> <" + fname.stringValue() + "> \"" + johnfname.stringValue() + "\"} }")
					.execute();
			Assert.assertEquals(3L, testAdminCon.size());

			testAdminCon.prepareUpdate("DELETE DATA {GRAPH <" + dirgraph.stringValue() + "> { <" + john.stringValue()
					+ "> <" + email.stringValue() + "> \"" + johnemail.stringValue() + "\"} }").execute();
			Assert.assertEquals(2L, testAdminCon.size());

			String query1 = "PREFIX ad: <http://marklogicsparql.com/addressbook#>" + " INSERT {GRAPH <"
					+ dirgraph.stringValue() + "> { <#1111> ad:email \"jsnelson@marklogic.com\"}}" + " where { GRAPH <"
					+ dirgraph.stringValue() + ">{<#1111> ad:lastName  ?name .} } ";

			testAdminCon.prepareUpdate(QueryLanguage.SPARQL, query1, "http://marklogicsparql.com/id").execute();
			Assert.assertEquals(3L, testAdminCon.size());
			testAdminCon.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		final Statement expSt = vf.createStatement(john, email, vf.createLiteral("jsnelson@marklogic.com"), dirgraph);
		Assert.assertEquals("Dirgraph's size must be 3", 3L, testAdminCon.size(dirgraph));
		testAdminCon.exportStatements(null, email, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				assertThat(st, equalTo(expSt));

			}
		}, dirgraph);

	}

	@Test
	public void testAddRemoveAdd() throws org.eclipse.rdf4j.RDF4JException {
		Assert.assertEquals(0L, testAdminCon.size());
		Statement st = vf.createStatement(john, lname, johnlname, dirgraph);
		testAdminCon.add(st);
		Assert.assertEquals(1L, testAdminCon.size());
		try {
			testAdminCon.begin();
			testAdminCon.remove(st, dirgraph);
			testAdminCon.add(st);
			testAdminCon.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		Assert.assertFalse(testAdminCon.isEmpty());
		Assert.assertEquals(1L, testAdminCon.size());
	}

	@Test
	public void testAddDeleteAdd() throws org.eclipse.rdf4j.RDF4JException {
		assertThat(testAdminCon.isOpen(), is(equalTo(true)));
		Assert.assertEquals(0L, testAdminCon.size());
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		try {
			testAdminCon.add(stmt);
			testAdminCon.begin();
			testAdminCon.prepareUpdate(QueryLanguage.SPARQL,
					"DELETE DATA {GRAPH <" + dirgraph.stringValue() + "> { <" + micah.stringValue() + "> <"
							+ homeTel.stringValue() + "> \"" + micahhomeTel.doubleValue()
							+ "\"^^<http://www.w3.org/2001/XMLSchema#double>} }")
					.execute();
			Thread.currentThread().sleep(2000L);
			Assert.assertTrue(testAdminCon.isEmpty());
			testAdminCon.add(stmt);
			testAdminCon.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		Assert.assertFalse(testAdminCon.isEmpty());
	}

	// ISSUE 133
	@Test
	public void testAddRemoveInsert() throws org.eclipse.rdf4j.RDF4JException {
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel);
		testAdminCon.add(stmt);
		try {
			testAdminCon.begin();
			testAdminCon.remove(stmt);
			Assert.assertEquals("The size of repository must be zero", 0, testAdminCon.size());
			testAdminCon.prepareUpdate(
					"INSERT DATA " + " { <" + micah.stringValue() + "> <#homeTel> \"" + micahhomeTel.doubleValue()
							+ "\"^^<http://www.w3.org/2001/XMLSchema#double>} ",
					"http://marklogicsparql.com/addressbook").execute();

			testAdminCon.commit();
		} catch (Exception e) {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
			Assert.assertTrue("Failed within transaction", 1 > 2);

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		Assert.assertFalse(testAdminCon.isEmpty());
		Assert.assertEquals(1L, testAdminCon.size());

	}

	// ISSSUE 106, 133
	@Test
	public void testAddDeleteInsertWhere() throws org.eclipse.rdf4j.RDF4JException {
		testAdminCon.add(fei, lname, feilname);
		testAdminCon.add(fei, email, feiemail);
		try {
			testAdminCon.begin();
			testAdminCon.prepareUpdate(
					" DELETE { <" + fei.stringValue() + "> <#email> \"" + feiemail.stringValue() + "\"} "
							+ " INSERT { <" + fei.stringValue()
							+ "> <#email> \"fling@marklogic.com\"} where{ ?s <#email> ?o}",
					"http://marklogicsparql.com/addressbook").execute();
			Assert.assertTrue("The value of email should be updated", testAdminCon
					.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false));
			testAdminCon.commit();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		Assert.assertTrue(testAdminCon
				.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false));
		Assert.assertFalse(testAdminCon.isEmpty());
	}

	@Test
	public void testGraphOps() throws Exception {
		IRI gr1 = vf.createIRI("http://marklogic.com");
		IRI gr2 = vf.createIRI("http://ml.com");
		testAdminCon.add(fei, lname, feilname);
		testAdminCon.add(fei, email, feiemail);
		try {
			testAdminCon.begin();
			testAdminCon.prepareUpdate(" CREATE GRAPH <http://marklogic.com> ").execute();
			testAdminCon.prepareUpdate(" CREATE GRAPH <http://ml.com> ").execute();
			Assert.assertTrue("The graph should be empty", (testAdminCon.size(gr1) == 0));
			Assert.assertTrue("The graph should be empty", (testAdminCon.size(gr2) == 0));
			testAdminCon.commit();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		testAdminCon.prepareUpdate(" COPY DEFAULT TO <http://marklogic.com> ").execute();
		Assert.assertFalse(testAdminCon.isEmpty());
		Assert.assertTrue("The graph gr1 should not be empty", (testAdminCon.size(gr1) == 2));

		testWriterCon.prepareUpdate(" MOVE DEFAULT TO <http://ml.com> ").execute();
		Assert.assertFalse(testWriterCon.isEmpty());
		Assert.assertTrue("The graph gr2 should not be empty", (testWriterCon.size(gr2) == 2));
		Assert.assertTrue("The graph gr2 should not be empty", (testAdminCon.size(gr2) == 2));
		// Assert.assertTrue("The default graph should be empty",
		// (testAdminCon.size(null) == 0));

		testWriterCon.prepareUpdate(" DROP GRAPH <http://ml.com> ").execute();
		testWriterCon.prepareUpdate(" DROP GRAPH <http://marklogic.com> ").execute();
		Assert.assertTrue("The default graph should  be empty", (testAdminCon.size() == 0));
	}

	@Test
	public void testAddDifferentFormats() throws Exception {
		testAdminCon.add(MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "journal.nt"),
				"", RDFFormat.NTRIPLES, dirgraph);
		Assert.assertEquals(36L, testAdminCon.size());
		testAdminCon.clear();

		testAdminCon.add(
				new InputStreamReader(
						MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "little.nq")),
				"", RDFFormat.NQUADS);
		Assert.assertEquals(9L, testAdminCon.size());
		testAdminCon.clear();

		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "semantics.trig");
		testAdminCon.add(url, "", RDFFormat.TRIG);
		Assert.assertEquals(15L, testAdminCon.size());
		testAdminCon.clear();

		File file = new File(
				MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "dir.json").getFile());
		testAdminCon.add(file, "", RDFFormat.RDFJSON);
		Assert.assertEquals(12L, testAdminCon.size());
		testAdminCon.clear();

		Reader fr = new FileReader(
				new File(MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "dir.xml").getFile()));
		testAdminCon.add(fr, "", RDFFormat.RDFXML);
		Assert.assertEquals(12L, testAdminCon.size());
		testAdminCon.clear();
	}

	// ISSUE 110
	@Test
	public void testOpen() throws Exception {
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		try {
			testAdminCon.begin();
			assertThat("testAdminCon should be open", testAdminCon.isOpen(), is(equalTo(true)));
			assertThat("testWriterCon should be open", testWriterCon.isOpen(), is(equalTo(true)));
			testAdminCon.add(stmt);
			testAdminCon.commit();
		} catch (Exception e) {

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		testAdminCon.remove(stmt, dirgraph);
		testAdminCon.close();

		try {
			testAdminCon.hasStatement(stmt, false, dirgraph);
			fail("Should not be able to run statements on testAdminCon");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		}

		try {
			testAdminCon.add(stmt);
			fail("Adding triples after close should not be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		}

		testAdminRepository.shutDown();
		testAdminRepository = null;
		testAdminCon = null;
		setUp();
		assertThat(testAdminCon.isOpen(), is(equalTo(true)));
		Assert.assertEquals("testAdminCon size should be zero", testAdminCon.size(), 0);
		assertThat("testWriterCon should be open", testWriterCon.isOpen(), is(equalTo(true)));
	}

	// ISSUE 126, 33
	@Test
	public void testClear() throws Exception {
		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, fname, feifname);
		assertThat(testAdminCon.hasStatement(null, null, null, false), is(equalTo(true)));
		testAdminCon.clear(dirgraph);
		Assert.assertFalse(testAdminCon.isEmpty());
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(false)));
		testAdminCon.clear();
		Assert.assertTrue(testAdminCon.isEmpty());
		assertThat(testAdminCon.hasStatement(null, null, null, false), is(equalTo(false)));
	}

	@Ignore
	public void testAddNullStatements() throws Exception {
		Statement st1 = vf.createStatement(john, fname, null, dirgraph);
		Statement st2 = vf.createStatement(null, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, null);
		Statement st4 = vf.createStatement(john, email, johnemail, null);
		Statement st5 = vf.createStatement(null, null, null, null);

		try {
			testAdminCon.add(st1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(e instanceof UnsupportedOperationException);
		}
		try {
			testAdminCon.add(st2);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UnsupportedOperationException);
		}
		try {
			testAdminCon.add(st3);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UnsupportedOperationException);
		}
		try {
			testAdminCon.add(st5);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UnsupportedOperationException);
		}

		testAdminCon.add(st4);
		Assert.assertEquals(1L, testAdminCon.size());
	}

	// ISSUE 65
	@Test
	public void testAddMalformedLiteralsDefaultConfig() throws Exception {
		try {
			testAdminCon.add(MarkLogicRepositoryConnectionTest.class
					.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"), "", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in default configuration");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RDFParseException);
		}
	}

	@Test
	public void testAddMalformedLiteralsStrictConfig() throws Exception {
		Assert.assertEquals(0L, testAdminCon.size());
		Set<RioSetting<?>> empty = Collections.emptySet();
		testAdminCon.getParserConfig().setNonFatalErrors(empty);

		try {
			testAdminCon.add(MarkLogicRepositoryConnectionTest.class
					.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"), "", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in strict configuration");

		} catch (Exception e) {
			Assert.assertTrue(e instanceof RDFParseException);

		}
	}

	// ISSUE 106, 132, 61, 126
	@Test
	public void testRemoveStatements() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(john, lname, johnlname, dirgraph);
			testAdminCon.add(john, fname, johnfname, dirgraph);
			testAdminCon.add(john, email, johnemail, dirgraph);
			testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
			testAdminCon.add(micah, lname, micahlname);
			testAdminCon.add(micah, fname, micahfname);
			testAdminCon.add(micah, homeTel, micahhomeTel);
			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		testAdminCon.setDefaultRulesets(null);
		Statement st1 = vf.createStatement(john, fname, johnlname);

		testAdminCon.remove(st1);

		Assert.assertEquals("There is no triple st1 in the repository, so it shouldn't be deleted", 7L,
				testAdminCon.size());

		Statement st2 = vf.createStatement(john, lname, johnlname);
		assertThat(testAdminCon.hasStatement(st2, false, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(st2, true, null, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(st2, true, (Resource) null), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, true, (Resource) null), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, true, (Resource) null, dirgraph, dirgraph1),
				is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(st2, true), is(equalTo(true)));
		testAdminCon.remove(st2, dirgraph);
		assertThat(testAdminCon.hasStatement(st2, true, null, dirgraph), is(equalTo(false)));

		Assert.assertEquals(6L, testAdminCon.size());

		testAdminCon.remove(john, email, null);
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, (Resource) null), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, null, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
		testAdminCon.remove(john, null, null);

		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, null, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, (Resource) null, dirgraph),
				is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, johnhomeTel, false, null, null), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, (Resource) null), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, null, (Resource) null),
				is(equalTo(true)));

		testAdminCon.remove((Resource) null, homeTel, (Value) null);
		testAdminCon.remove((Resource) null, homeTel, (Value) null);

		testAdminCon.remove(vf.createStatement(john, lname, johnlname), dirgraph);
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
		testAdminCon.add(john, fname, johnfname, dirgraph);

		assertThat(testAdminCon.hasStatement(john, homeTel, johnhomeTel, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false), is(equalTo(false)));

		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, dirgraph), is(equalTo(true)));

		testAdminCon.remove(john, (IRI) null, (Value) null);
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.isEmpty(), is(equalTo(false)));

		testAdminCon.remove(null, null, micahlname);
		assertThat(testAdminCon.hasStatement(micah, fname, micahfname, false), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(micah, fname, micahfname, false, null, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(null, null, null, false, null, null), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(null, null, null, false), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(null, null, null, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, fname, micahfname, false, dirgraph1, dirgraph), is(equalTo(false)));
		testAdminCon.remove((IRI) null, null, null);
		assertThat(testAdminCon.isEmpty(), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement((IRI) null, (IRI) null, (Literal) null, false, (Resource) null),
				is(equalTo(false)));
	}

	// ISSUE 130
	@Test
	public void testRemoveStatementCollection() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(john, lname, johnlname);
			testAdminCon.add(john, fname, johnfname);
			testAdminCon.add(john, email, johnemail);
			testAdminCon.add(john, homeTel, johnhomeTel);
			testAdminCon.add(micah, lname, micahlname, dirgraph);
			testAdminCon.add(micah, fname, micahfname, dirgraph);
			testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph);
			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, null, dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, dirgraph), is(equalTo(true)));

		Collection<Statement> c = Iterations.addAll(testAdminCon.getStatements(null, null, null, false),
				new ArrayList<Statement>());

		testAdminCon.remove(c);

		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.isEmpty(), is(equalTo(true)));
	}

	// ISSUE 130
	@Test
	public void testRemoveStatementIterable() throws Exception {
		testAdminCon.add(john, fname, johnfname);
		Statement st1 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st2 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st3 = vf.createStatement(fei, email, feiemail, dirgraph);

		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		StatementIterator iter = new StatementIterator(sL);
		Iterable<? extends Statement> iterable = new StatementIterable(iter);

		testAdminCon.add(iterable);
		Assert.assertEquals(4L, testAdminCon.size());

		StatementList<Statement> sL1 = new StatementList<Statement>(st1);
		sL1.add(st2);
		sL1.add(st3);
		StatementIterator iter1 = new StatementIterator(sL1);
		Iterable<? extends Statement> iterable1 = new StatementIterable(iter1);
		Assert.assertTrue(iterable1.iterator().hasNext());

		testAdminCon.remove(iterable1, dirgraph);
		Assert.assertEquals(1L, testAdminCon.size());
	}

	// ISSUE 66
	@Test
	public void testRemoveStatementIteration() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(john, fname, johnfname);
			testAdminCon.add(fei, fname, feifname, dirgraph);
			testAdminCon.add(fei, lname, feilname, dirgraph);
			testAdminCon.add(fei, email, feiemail, dirgraph);
			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		Assert.assertEquals(4L, testAdminCon.size());

		Statement st1 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st2 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st3 = vf.createStatement(fei, email, feiemail, dirgraph);

		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		StatementIterator iter = new StatementIterator(sL);
		Iteration<Statement, Exception> it = new IteratorIteration<Statement, Exception>(iter);
		Assert.assertTrue(it.hasNext());
		testAdminCon.remove(it);
		Assert.assertEquals(1L, testAdminCon.size());
	}

	// ISSUE 118, 129
	@Test
	public void testGetStatements() throws Exception {
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, lname, johnlname);
		testAdminCon.add(john, homeTel, johnhomeTel);
		testAdminCon.add(john, email, johnemail);

		try {
			assertTrue("Repository should contain statement",
					testAdminCon.hasStatement(john, homeTel, johnhomeTel, false));
		} catch (Exception e) {
			logger.debug(e.getMessage());
		}

		RepositoryResult<Statement> result = testAdminCon.getStatements(null, homeTel, null, false);

		try {
			assertNotNull("Iterator should not be null", result);
			assertTrue("Iterator should not be empty", result.hasNext());

			while (result.hasNext()) {
				Statement st = result.next();
				Assert.assertNull("Statement should not be in a context ", st.getContext());
				assertTrue("Statement predicate should be equal to name ", st.getPredicate().equals(homeTel));
			}
		} finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testAdminCon.getStatements(null, john, null, false, dirgraph),
				new ArrayList<Statement>());

		assertTrue("List should be empty", list.isEmpty());

		testAdminCon.clear();
		Assert.assertTrue(testAdminCon.isEmpty());

		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph1);
		testAdminCon.add(john, homeTel, johnhomeTel, (Resource) null);
		testAdminCon.add(john, email, johnemail);
		result = testAdminCon.getStatements(null, null, null, false);
		int count = 0;
		try {
			assertNotNull("Iterator should not be null", result);
			assertTrue("Iterator should not be empty", result.hasNext());
			while (result.hasNext()) {
				count++;
				Statement st = result.next();
				if (st.getContext() == null) {
					assertTrue(st.getPredicate().equals(email) || st.getPredicate().equals(homeTel));
				} else {
					if (st.getContext().equals(dirgraph)) {
						assertTrue(st.getObject().equals(johnfname));
					} else if (st.getContext().equals(dirgraph1)) {
						assertTrue(st.getObject().equals(johnlname));
					}
				}
			}
			Assert.assertTrue(count == 4);
		} finally {
			result.close();
		}

		result = testAdminCon.getStatements((Resource) null, (IRI) null, (Value) null);
		count = 0;
		try {
			assertNotNull("Iterator should not be null", result);
			assertTrue("Iterator should not be empty", result.hasNext());
			while (result.hasNext()) {
				count++;
				Statement st = result.next();
				if (st.getContext() == null) {
					assertTrue(st.getPredicate().equals(email) || st.getPredicate().equals(homeTel));
				} else {
					if (st.getContext().equals(dirgraph)) {
						assertTrue(st.getObject().equals(johnfname));
					} else if (st.getContext().equals(dirgraph1)) {
						assertTrue(st.getObject().equals(johnlname));
					}
				}
			}
			Assert.assertTrue(count == 4);
		} finally {
			result.close();
		}
	}

	// ISSUE 131
	@Test
	public void testGetStatementsMalformedTypedLiteral() throws Exception {

		testAdminCon.getParserConfig().addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
		Literal invalidIntegerLiteral = vf.createLiteral("four", XMLSchema.INTEGER);
		try {
			testAdminCon.add(micah, homeTel, invalidIntegerLiteral, dirgraph);

			RepositoryResult<Statement> statements = testAdminCon.getStatements(micah, homeTel, null, true);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(invalidIntegerLiteral));
		} catch (RepositoryException e) {
			// shouldn't happen
			fail(e.getMessage());
		}
	}

	// ISSUE 131, 178
	@Test
	public void testGetStatementsLanguageLiteral() throws Exception {
		Literal validLanguageLiteral = vf.createLiteral("the number four", "en");
		try {
			testAdminCon.add(micah, homeTel, validLanguageLiteral, dirgraph);

			RepositoryResult<Statement> statements = testAdminCon.getStatements(null, null, null, true, dirgraph);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(validLanguageLiteral));
		} catch (RepositoryException e) {
			// shouldn't happen
			e.printStackTrace();
			fail(e.getMessage());
		}

		// Uncomment after 178 is fixed.
		/*
		 * testAdminCon.clear(); Literal invalidLanguageLiteral =
		 * vf.createLiteral("the number four", "en_us"); try {
		 * testAdminCon.add(micah, homeTel, invalidLanguageLiteral,dirgraph);
		 * 
		 * RepositoryResult<Statement> statements =
		 * testAdminCon.getStatements(null, null, null, true,dirgraph);
		 * 
		 * assertNotNull(statements); assertTrue(statements.hasNext());
		 * Statement st = statements.next(); assertTrue(st.getObject()
		 * instanceof Literal);
		 * assertTrue(st.getObject().equals(invalidLanguageLiteral)); } catch
		 * (RepositoryException e) { // shouldn't happen fail(e.getMessage()); }
		 */
	}

	// ISSUE 26 , 83, 90, 106, 107, 120, 81
	@Test
	public void testGetStatementsInSingleContext() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(micah, fname, micahfname, dirgraph1);
			testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);

			testAdminCon.add(john, fname, johnfname, dirgraph);
			testAdminCon.add(john, lname, johnlname, dirgraph);
			testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);

			Assert.assertEquals("Size of dirgraph1 must be 3", 3, testAdminCon.size(dirgraph1));
			Assert.assertEquals("Size of unknown context must be 0", 0L, testAdminCon.size(vf.createIRI(":asd")));
			Assert.assertEquals("Size of dirgraph must be 3", 3, testAdminCon.size(dirgraph));
			Assert.assertEquals("Size of repository must be 6", 6, testAdminCon.size());
			Assert.assertEquals("Size of repository must be 6", 6, testAdminCon.size(dirgraph, dirgraph1, null));
			Assert.assertEquals("Size of repository must be 6", 6, testAdminCon.size(dirgraph, dirgraph1));
			Assert.assertEquals("Size of repository must be 3", 3, testAdminCon.size(dirgraph, null));
			Assert.assertEquals("Size of repository must be 3", 3, testAdminCon.size(dirgraph1, null));
			Assert.assertEquals("Size of default graph must be 0", 0, testAdminCon.size(null, null));

			testAdminCon.add(dirgraph, vf.createIRI("http://TYPE"), vf.createLiteral("Directory Graph"));
			Assert.assertEquals("Size of default graph must be 1", 1, testAdminCon.size((Resource) null));
			Assert.assertEquals("Size of repository must be 4", 4, testAdminCon.size(dirgraph, null));
			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (testAdminCon.isActive()) {
				testAdminCon.rollback();
			}
		}
		Assert.assertEquals("Size of repository must be 4", 4, testAdminCon.size(dirgraph1, null));
		Assert.assertEquals(1, testAdminCon.size((Resource) null));
		Assert.assertEquals(1, testAdminCon.size(null, null));
		Assert.assertEquals(3, testAdminCon.size(dirgraph, dirgraph));

		assertTrue("Repository should contain statement", testAdminCon.hasStatement(john, homeTel, johnhomeTel, false));
		assertTrue("Repository should contain statement in dirgraph1",
				testAdminCon.hasStatement(micah, lname, micahlname, false, dirgraph1));
		assertFalse("Repository should not contain statement in context2",
				testAdminCon.hasStatement(micah, lname, micahlname, false, dirgraph));

		// Check handling of getStatements without context IDs
		RepositoryResult<Statement> result = testAdminCon.getStatements(micah, lname, null, false);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getSubject(), is(equalTo((Resource) micah)));
				assertThat(st.getPredicate(), is(equalTo(lname)));
				assertThat(st.getObject(), is(equalTo((Value) micahlname)));
				assertThat(st.getContext(), is(equalTo((Resource) dirgraph1)));
			}
		} finally {
			result.close();
		}

		// Check handling of getStatements with a known context ID
		result = testAdminCon.getStatements(null, null, null, false, dirgraph);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getContext(), is(equalTo((Resource) dirgraph)));
			}
		} finally {
			result.close();
		}

		// Check handling of getStatements with null context
		result = testAdminCon.getStatements(null, null, null, false, null, null);
		assertThat(result, is(notNullValue()));
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getContext(), is(equalTo((Resource) null)));
			}
		} finally {
			result.close();
		}

		// Check handling of getStatements with an unknown context ID
		result = testAdminCon.getStatements(null, null, null, false, vf.createIRI(":unknownContext"));
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(false)));
		} finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testAdminCon.getStatements(null, lname, null, false, dirgraph1),
				new ArrayList<Statement>());
		assertNotNull("List should not be null", list);
		assertFalse("List should not be empty", list.isEmpty());

		List<Statement> list1 = Iterations.addAll(
				testAdminCon.getStatements(dirgraph, null, null, false, (Resource) null), new ArrayList<Statement>());
		assertNotNull("List should not be null", list1);
		assertFalse("List should not be empty", list1.isEmpty());

	}

	// ISSUE 82, 127, 129, 140
	@Test
	public void testGetStatementsInMultipleContexts() throws Exception {
		IRI ur = vf.createIRI("http://abcd");

		CloseableIteration<? extends Statement, RepositoryException> iter1 = testAdminCon.getStatements(null, null,
				null, false, null, null);

		try {
			int count = 0;
			while (iter1.hasNext()) {
				iter1.next();
				count++;
			}
			assertEquals("there should be 0 statements", 0, count);
		} finally {
			iter1.close();
			iter1 = null;
		}

		testAdminCon.begin();
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);
		testAdminCon.add(dirgraph1, ur, vf.createLiteral("test"));
		testAdminCon.commit();

		// get statements with either no context or dirgraph1
		CloseableIteration<? extends Statement, RepositoryException> iter = testAdminCon.getStatements(null, null, null,
				false, null, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource) dirgraph1))));
			}

			assertEquals("there should be four statements", 4, count);
		} finally {
			iter.close();
			iter = null;
		}

		iter = testAdminCon.getStatements(null, null, null, false, dirgraph1, dirgraph);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), is(equalTo((Resource) dirgraph1)));
			}
			assertEquals("there should be three statements", 3, count);
		} finally {
			iter.close();
			iter = null;
		}

		// get all statements with unknownContext or context2.
		IRI unknownContext = testAdminCon.getValueFactory().createIRI("http://unknownContext");
		iter = testAdminCon.getStatements(null, null, null, false, unknownContext, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				Statement st = iter.next();
				count++;
				assertThat(st.getContext(), is(equalTo((Resource) dirgraph1)));
			}
			assertEquals("there should be three statements", 3, count);
		} finally {
			iter.close();
			iter = null;
		}

		// add statements to dirgraph
		try {
			testAdminCon.begin();
			testAdminCon.add(john, fname, johnfname, dirgraph);
			testAdminCon.add(john, lname, johnlname, dirgraph);
			testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
			testAdminCon.commit();
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}

		// get statements with either no context or dirgraph
		iter = testAdminCon.getStatements(null, null, null, false, null, dirgraph);
		try {
			assertThat(iter, is(notNullValue()));
			assertThat(iter.hasNext(), is(equalTo(true)));
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource) dirgraph))));
			}
			assertEquals("there should be four statements", 4, count);
		} finally {
			iter.close();
			iter = null;
		}

		// get all statements with dirgraph or dirgraph1
		iter = testAdminCon.getStatements(null, null, null, false, null, dirgraph, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource) dirgraph)),
						is(equalTo((Resource) dirgraph1))));
			}
			assertEquals("there should be 7 statements", 7, count);
		} finally {
			iter.close();
			iter = null;
		}

		// get all statements with dirgraph or dirgraph1
		iter = testAdminCon.getStatements(null, null, null, false, dirgraph, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(equalTo((Resource) dirgraph)), is(equalTo((Resource) dirgraph1))));
			}
			assertEquals("there should be 6 statements", 6, count);
		} finally {
			iter.close();
			iter = null;
		}

	}

	@Test
	public void testPagination() throws Exception {

		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX  bb: <http://marklogic.com/baseball/players#> ");
		queryBuilder.append(" PREFIX  r: <http://marklogic.com/baseball/rules#> ");
		queryBuilder.append(" SELECT ?id ?lastname  ");
		queryBuilder.append("  {  ");
		queryBuilder.append(" ?id bb:lastname ?lastname. ");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?lastname");

		MarkLogicQuery query = testAdminCon.prepareQuery(queryBuilder.toString());

		TupleQueryResult result1 = ((MarkLogicTupleQuery) query).evaluate(1, 2);
		String[] expLname = { "Ausmus", "Avila", "Bernard", "Cabrera", "Carrera", "Castellanos", "Holaday", "Joyner",
				"Lamont", "Nathan", "Verlander" };
		String[] expID = { "http://marklogic.com/baseball/players#157", "http://marklogic.com/baseball/players#120",
				"http://marklogic.com/baseball/players#130", "http://marklogic.com/baseball/players#123",
				"http://marklogic.com/baseball/players#131", "http://marklogic.com/baseball/players#124",
				"http://marklogic.com/baseball/players#121", "http://marklogic.com/baseball/players#159",
				"http://marklogic.com/baseball/players#158", "http://marklogic.com/baseball/players#107",
				"http://marklogic.com/baseball/players#119" };
		int i = 0;
		while (result1.hasNext()) {
			BindingSet solution = result1.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[i], totalResult.stringValue());
			i++;
		}
		Assert.assertEquals(2, i);

		i = 0;
		TupleQueryResult result2 = ((MarkLogicTupleQuery) query).evaluate(1, 0);
		while (result2.hasNext()) {
			BindingSet solution = result2.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[i], totalResult.stringValue());
			logger.debug("String values : " + expLname[i]);
			i++;
		}

		try {
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(0, 0);
			Assert.assertTrue(2 > 1);
		} catch (Exception e) {
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(-1, -1);
			Assert.assertTrue(2 > 1);
		} catch (Exception e) {
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(2, -1);
			Assert.assertTrue(2 > 1);
		} catch (Exception e) {
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(-2, 2);
			Assert.assertTrue(2 > 1);
		} catch (Exception e) {
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		i = 0;
		TupleQueryResult result4 = ((MarkLogicTupleQuery) query).evaluate(11, 2);
		while (result4.hasNext()) {
			BindingSet solution = result4.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[11 - i - 1], totalResult.stringValue());

			i++;
		}
		Assert.assertEquals(1L, i);
	}

	// ISSUE 72
	@Test
	public void testPrepareNonSparql() throws Exception {

		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		Assert.assertEquals(107L, testAdminCon.size());

		String query1 = "ASK " + "WHERE" + "{" + " ?s <#position> ?o." + "}";

		try {
			testAdminCon.prepareGraphQuery(QueryLanguage.SERQL, query1, "http://marklogic.com/baseball/players")
					.evaluate();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (UnsupportedQueryLanguageException ex) {
			Assert.assertEquals("Unsupported query language SeRQL", ex.getMessage());
		}

		try {

			testAdminCon.prepareTupleQuery(QueryLanguage.SERQO, query1).evaluate(1, 2);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (UnsupportedQueryLanguageException ex1) {
			Assert.assertEquals("Unsupported query language SeRQO", ex1.getMessage());
		}
		try {
			testAdminCon.prepareBooleanQuery(QueryLanguage.SERQL, query1).evaluate();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (UnsupportedQueryLanguageException ex1) {
			Assert.assertEquals("Unsupported query language SeRQL", ex1.getMessage());
		}
		try {
			testAdminCon.prepareUpdate(QueryLanguage.SERQO, query1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (UnsupportedQueryLanguageException ex1) {
			Assert.assertEquals("Unsupported query language SeRQO", ex1.getMessage());
		}

		try {
			testAdminCon.prepareQuery(QueryLanguage.SERQL, query1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1 < 2);
		} catch (UnsupportedQueryLanguageException ex1) {
			Assert.assertEquals("Unsupported query language SeRQL", ex1.getMessage());
		}

	}

	// ISSUE 73
	@Test
	public void testPrepareInvalidSparql() throws Exception {
		Assert.assertEquals(0L, testWriterCon.size());
		Assert.assertTrue(testAdminCon.isEmpty());

		Statement st1 = vf.createStatement(john, fname, johnfname);
		testWriterCon.add(st1, dirgraph);
		Assert.assertEquals(1L, testWriterCon.size(dirgraph));

		String query = " DESCRIBE  <http://marklogicsparql.com/id#1111>  ";

		try {
			boolean tq = testReaderCon.prepareBooleanQuery(query, "http://marklogicsparql.com/id").evaluate();
			Assert.assertEquals(0L, 1L);

		}
		// Change exception IIlegalArgumentException
		catch (Exception ex1) {
			Assert.assertTrue(ex1 instanceof Exception);
		}

		String query1 = "ASK {" + "{" + " ?s <#position> ?o." + "}";
		try {
			boolean tq = testReaderCon.prepareBooleanQuery(query1, "http://marklogicsparql.com/id").evaluate();
			Assert.assertEquals(0L, 1L);

		}
		// Should be MalformedQueryException
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.assertTrue(ex instanceof Exception);
		}

	}

	// ISSUE # 133, 183
	@Test
	public void testUnsupportedIsolationLevel() throws Exception {
		Assert.assertEquals(testAdminCon.size(), 0L);
		Assert.assertEquals(IsolationLevels.SNAPSHOT, testAdminCon.getIsolationLevel());
		try {
			testAdminCon.begin();
			testAdminCon.add(john, fname, johnfname);
			assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
			assertThat(testWriterCon.hasStatement(john, fname, johnfname, false), is(equalTo(false)));
			testAdminCon.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
		assertThat(testWriterCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
		try {
			testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT_READ);
			Assert.assertTrue(1 > 2);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalStateException);
		}

	}

	// ISSUE # 252
	@Test
	public void testIsolationLevel() throws Exception {
		RepositoryConnection repConn = null;
		Repository tempRep1 = null;
		try {
			MarkLogicRepositoryConfig tempConfig1 = new MarkLogicRepositoryConfig();
			tempConfig1.setHost(host);
			tempConfig1.setAuth("DIGEST");
			tempConfig1.setUser("admin");
			tempConfig1.setPassword("admin");
			tempConfig1.setPort(restPort);
			tempRep1 = new MarkLogicRepositoryFactory().getRepository(tempConfig1);
			tempRep1.initialize();
			repConn = tempRep1.getConnection();
			repConn.begin();
			repConn.add(john, fname, johnfname);
			createRepconn();
			assertThat(repConn.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
			repConn.commit();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (repConn.isActive())
				repConn.rollback();
			tempRep1.shutDown();
			repConn.close();
			repConn = null;
			tempRep1 = null;
		}

	}

	private void createRepconn() throws Exception {
		RepositoryConnection repConn1 = null;
		Repository tempRep2 = null;
		try {
			MarkLogicRepositoryConfig tempConfig2 = new MarkLogicRepositoryConfig();
			tempConfig2.setHost(host);
			tempConfig2.setAuth("DIGEST");
			tempConfig2.setUser("admin");
			tempConfig2.setPassword("admin");
			tempConfig2.setPort(restPort);
			tempRep2 = new MarkLogicRepositoryFactory().getRepository(tempConfig2);
			tempRep2.initialize();
			repConn1 = tempRep2.getConnection();
			assertThat(repConn1.hasStatement(john, fname, johnfname, false), is(equalTo(false)));
		} catch (Exception e) {
			logger.debug(e.getMessage());
		} finally {
			if (repConn1.isActive())
				repConn1.rollback();
			tempRep2.shutDown();
			repConn1.close();
			repConn1 = null;
			tempRep2 = null;
		}

	}

	// ISSUE - 84
	@Test
	public void testNoUpdateRole() throws Exception {
		try {
			testAdminCon.prepareUpdate("DROP GRAPH <abc>").execute();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UpdateExecutionException);
		}

		try {
			testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UpdateExecutionException);
		}

		testAdminCon.prepareUpdate("CREATE GRAPH <http://abc>").execute();

		final Statement st1 = vf.createStatement(john, fname, johnfname);

		try {
			testReaderCon.add(st1, vf.createIRI("http://abc"));
		} catch (Exception e) {
			Assert.assertTrue(e instanceof UpdateExecutionException);
		}

		try {
			testReaderCon.begin();
			testReaderCon.add(st1, vf.createIRI("http://abc"));
			testReaderCon.commit();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		} finally {
			if (testReaderCon.isActive())
				testReaderCon.rollback();
		}
		try {
			testReaderCon.close();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof RepositoryException);
		} finally {
			if (testReaderCon.isActive())
				testReaderCon.rollback();
		}
	}

	@Test
	public void testRuleSets1() throws Exception {
		Assert.assertEquals(0L, testAdminCon.size());
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, developPrototypeOf, semantics, dirgraph1);
		testAdminCon.add(micah, type, sEngineer, dirgraph1);
		testAdminCon.add(micah, worksFor, ml, dirgraph1);

		testAdminCon.add(john, fname, johnfname, dirgraph1);
		testAdminCon.add(john, lname, johnlname, dirgraph1);
		testAdminCon.add(john, writeFuncSpecOf, inference, dirgraph1);
		testAdminCon.add(john, type, lEngineer, dirgraph1);
		testAdminCon.add(john, worksFor, ml, dirgraph1);

		testAdminCon.add(writeFuncSpecOf, eqProperty, design, dirgraph1);
		testAdminCon.add(developPrototypeOf, eqProperty, design, dirgraph1);
		testAdminCon.add(design, eqProperty, develop, dirgraph1);

		testAdminCon.setDefaultRulesets(SPARQLRuleset.EQUIVALENT_PROPERTY);

		Assert.assertTrue(testAdminCon.hasStatement(john, design, inference, true, dirgraph1));
		Assert.assertTrue(testAdminCon.hasStatement(john, design, inference, true));

		// ISSUE # RDFJ -30
		Assert.assertFalse(testAdminCon.hasStatement(john, design, inference, false, dirgraph1));
		Assert.assertFalse(testAdminCon.hasStatement(john, design, inference, false));

		Assert.assertFalse(testAdminCon.hasStatement(john, design, inference, true, dirgraph));
		Assert.assertFalse(testAdminCon.hasStatement(john, design, inference, false, dirgraph));

		TupleQuery tQ = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL,
				"select (count (?s)  as ?totalcount) where {?s ?p ?o .} ");
		((MarkLogicQuery) tQ).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		TupleQueryResult resulttQ = tQ.evaluate();
		// returns 25 triples by applying Equivalent.class and
		// Equivalent.property ruleset
		try {
			assertThat(resulttQ, is(notNullValue()));
			assertThat(resulttQ.hasNext(), is(equalTo(true)));
			while (resulttQ.hasNext()) {
				BindingSet solution = resulttQ.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(25, Integer.parseInt(count.stringValue()));
			}
			Assert.assertTrue(testAdminCon.hasStatement(john, design, inference, true, dirgraph1));
			Assert.assertFalse(testAdminCon.hasStatement(john, design, inference, false, dirgraph1));
		} finally {
			resulttQ.close();
		}

		testAdminCon.setDefaultRulesets(null);

		String query = "select  (count (?s)  as ?totalcount) where {?s ?p ?o .} ";
		TupleQuery tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		TupleQueryResult result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(15, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		testAdminCon.setDefaultRulesets(SPARQLRuleset.EQUIVALENT_PROPERTY, null);
		TupleQuery tupleQuery1 = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult result1 = tupleQuery1.evaluate();

		try {
			assertThat(result1, is(notNullValue()));
			assertThat(result1.hasNext(), is(equalTo(true)));
			while (result1.hasNext()) {
				BindingSet solution = result1.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(23, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result1.close();
		}

		SPARQLRuleset[] ruleset = testAdminCon.getDefaultRulesets();
		Assert.assertEquals(2, ruleset.length);
		Assert.assertEquals(ruleset[0], SPARQLRuleset.EQUIVALENT_PROPERTY);
		Assert.assertEquals(ruleset[1], null);

		testAdminCon.setDefaultRulesets(null);

		TupleQuery tupleQuery2 = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery2).setRulesets(null);
		tupleQuery2.setIncludeInferred(false);
		TupleQueryResult result2 = tupleQuery2.evaluate();

		try {
			assertThat(result2, is(notNullValue()));
			assertThat(result2.hasNext(), is(equalTo(true)));
			while (result2.hasNext()) {
				BindingSet solution = result2.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(13, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result2.close();
		}
	}

	// ISSUE 128, 163, 111, 112 (closed)
	@Test
	public void testRuleSets2() throws Exception {

		Assert.assertEquals(0L, testAdminCon.size());
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, developPrototypeOf, semantics, dirgraph1);
		testAdminCon.add(micah, type, sEngineer, dirgraph1);
		testAdminCon.add(micah, worksFor, ml, dirgraph1);

		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, writeFuncSpecOf, inference, dirgraph);
		testAdminCon.add(john, type, lEngineer, dirgraph);
		testAdminCon.add(john, worksFor, ml, dirgraph);

		testAdminCon.add(writeFuncSpecOf, subProperty, design, dirgraph1);
		testAdminCon.add(developPrototypeOf, subProperty, design, dirgraph1);
		testAdminCon.add(design, subProperty, develop, dirgraph1);

		testAdminCon.add(lEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(sEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(engineer, subClass, employee, dirgraph1);

		String query = "select (count (?s)  as ?totalcount)  where {?s ?p ?o .} ";
		TupleQuery tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		TupleQueryResult result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(374, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		RepositoryResult<Statement> resultg = testAdminCon.getStatements(null, null, null, true, dirgraph, dirgraph1);

		assertNotNull("Iterator should not be null", resultg);
		assertTrue("Iterator should not be empty", resultg.hasNext());

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(18, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS, SPARQLRuleset.INVERSE_OF);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(86, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(null, SPARQLRuleset.INVERSE_OF);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(18, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets((SPARQLRuleset) null, null);
		tupleQuery.setIncludeInferred(false);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(16, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}
	}
	
	@Test
	public void testRuleSetswithOptimize() throws Exception {

		Assert.assertEquals(0L, testAdminCon.size());
		//Assert.assertEquals(1,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.setOptimizeLevel(1);
		Assert.assertEquals(1,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.setOptimizeLevel(2);
		Assert.assertEquals(2,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.setOptimizeLevel(0);
		Assert.assertEquals(0,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.add(micah, developPrototypeOf, semantics, dirgraph1);
		testAdminCon.setOptimizeLevel(100);
		Assert.assertEquals(100,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.add(micah, type, sEngineer, dirgraph1);
		testAdminCon.add(micah, worksFor, ml, dirgraph1);
		testAdminCon.setOptimizeLevel(-4);
		Assert.assertEquals(-4,testAdminCon.getOptimizeLevel().intValue());
		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, writeFuncSpecOf, inference, dirgraph);
		testAdminCon.add(john, type, lEngineer, dirgraph);
		testAdminCon.add(john, worksFor, ml, dirgraph);

		testAdminCon.add(writeFuncSpecOf, subProperty, design, dirgraph1);
		testAdminCon.add(developPrototypeOf, subProperty, design, dirgraph1);
		testAdminCon.add(design, subProperty, develop, dirgraph1);

		testAdminCon.add(lEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(sEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(engineer, subClass, employee, dirgraph1);

		String query = "select (count (?s)  as ?totalcount)  where {?s ?p ?o .} ";
		TupleQuery tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		testAdminCon.setOptimizeLevel(2);
		TupleQueryResult result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(374, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}
		Assert.assertEquals(2,testAdminCon.getOptimizeLevel().intValue());
		
		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		testAdminCon.setOptimizeLevel(1);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(374, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}
		Assert.assertEquals(1, testAdminCon.getOptimizeLevel().intValue());
		
		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		testAdminCon.setOptimizeLevel(0);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(374, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}
		Assert.assertEquals(0,testAdminCon.getOptimizeLevel().intValue());
		
		testAdminCon.setOptimizeLevel(-25);
		RepositoryResult<Statement> resultg = testAdminCon.getStatements(null, null, null, true, dirgraph, dirgraph1);

		assertNotNull("Iterator should not be null", resultg);
		assertTrue("Iterator should not be empty", resultg.hasNext());

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		result = tupleQuery.evaluate();
		Assert.assertEquals(-25,testAdminCon.getOptimizeLevel().intValue());
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(18, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS, SPARQLRuleset.INVERSE_OF);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(86, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(null, SPARQLRuleset.INVERSE_OF);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(18, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets((SPARQLRuleset) null, null);
		tupleQuery.setIncludeInferred(false);
		result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(16, Integer.parseInt(count.stringValue()));
			}
		} finally {
			result.close();
		}
	}
	
	@Test
	public void testPrepareBooleanQuerywithOptimize() throws Exception {

		File file = new File(
				MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + "tigers.ttl").getFile());
		testAdminCon.add(file, "", RDFFormat.TURTLE, graph1);
		logger.debug(file.getAbsolutePath());
		testAdminCon.setOptimizeLevel(2);
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		Assert.assertEquals(2,testAdminCon.getOptimizeLevel().intValue());
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"
				+ "ASK FROM <http://marklogic.com/Graph1>" + "WHERE" + "{" + "<#119> <#lastname> \"Verlander\"."
				+ "<#119> <#team> ?tigers." + "}";

		boolean result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
		
		testAdminCon.setOptimizeLevel(0);
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		Assert.assertEquals(0,testAdminCon.getOptimizeLevel().intValue());
		
		result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
		
		testAdminCon.setOptimizeLevel(1);
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		Assert.assertEquals(1,testAdminCon.getOptimizeLevel().intValue());
		result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
		
		testAdminCon.setOptimizeLevel(100);
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		Assert.assertEquals(100,testAdminCon.getOptimizeLevel().intValue());
		result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
		
		testAdminCon.setOptimizeLevel(-25);
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		Assert.assertEquals(-25,testAdminCon.getOptimizeLevel().intValue());
		result1 = testAdminCon.prepareBooleanQuery(query1, "http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);
	}

	
	@Test
	public void testConstrainingQueries() throws Exception {

		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, lname, johnlname);

		String query1 = "ASK WHERE {?s ?p \"Micah\" .}";
		String query2 = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .} ORDER by ?o";

		// case one, rawcombined
		String combinedQuery = "{\"search\":" + "{\"qtext\":\"2222\"}}";
		String negCombinedQuery = "{\"search\":" + "{\"qtext\":\"John\"}}";

		RawCombinedQueryDefinition rawCombined = qmgr
				.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
		RawCombinedQueryDefinition negRawCombined = qmgr
				.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

		MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,
				query1);
		askQuery.setConstrainingQueryDefinition(rawCombined);
		Assert.assertEquals(true, askQuery.evaluate());

		testAdminCon.setDefaultConstrainingQueryDefinition(negRawCombined);
		MarkLogicTupleQuery tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query2);
		TupleQueryResult result = tupleQuery.evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value fname = solution.getValue("o");
				String name = fname.stringValue().toString();
				Assert.assertTrue(name.equals("John") || name.equals("Snelson"));

			}
		} finally {
			result.close();
		}
		QueryDefinition qd = testAdminCon.getDefaultConstrainingQueryDefinition();
		testAdminCon.setDefaultConstrainingQueryDefinition(null);
		testAdminCon.setDefaultConstrainingQueryDefinition(qd);

		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query2);
		result = tupleQuery.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value fname = solution.getValue("o");
				String name = fname.stringValue().toString();
				Assert.assertTrue(name.equals("John") || name.equals("Snelson"));
			}
		} finally {
			result.close();
		}

		testAdminCon.setDefaultConstrainingQueryDefinition(null);
		tupleQuery = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query2);
		result = tupleQuery.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value fname = solution.getValue("o");
				String name = fname.stringValue().toString();
				System.out.println("Name: " + name);
				Assert.assertTrue(name.equals("John") || name.equals("Snelson") || name.equals("Micah")
						|| name.equals("Dubinko"));
			}
		} finally {
			result.close();
		}
	}

	// ISSUE 124, 142
	@Test
	public void testStructuredQuery() throws Exception {
		try {
			setupData();
			StructuredQueryBuilder qb = new StructuredQueryBuilder();
			QueryDefinition structuredDef = qb.build(qb.term("Second"));

			String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
			String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";

			MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(structuredDef);
			Assert.assertEquals(true, askQuery.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(structuredDef);
			MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(false, askQuery1.evaluate());

			QueryDefinition qd = testAdminCon.getDefaultConstrainingQueryDefinition();

			testAdminCon.setDefaultConstrainingQueryDefinition(null);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(true, askQuery1.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(qd);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(false, askQuery1.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(null);

			askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(null);
			Assert.assertEquals(true, askQuery.evaluate());

			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			askQuery.setConstrainingQueryDefinition(null);
			Assert.assertEquals(true, askQuery1.evaluate());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		} finally {
			clearData();
		}
	}

	@Test
	public void testQuerywithOptions() throws Exception {
		try {
			addRangeElementIndex(dbName, "int", "", "popularity");
			Thread.currentThread().sleep(2000L);
			DatabaseClient dbClient = DatabaseClientFactory.newClient(host, restPort, "admin", "admin",
					DatabaseClientFactory.Authentication.valueOf("DIGEST"));
			setupData();

			Thread.currentThread().sleep(2000L);
			String option = "setViewOpt.xml";
			writeQueryOption(dbClient, option);

			StringQueryDefinition stringDef = qmgr.newStringDefinition();
			stringDef.setOptionsName(option);
			stringDef.withCriteria("pop:high");

			String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
			String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";

			MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(stringDef);
			Assert.assertEquals(true, askQuery.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef);
			MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(false, askQuery1.evaluate());

			StringQueryDefinition stringDef1 = qmgr.newStringDefinition();
			stringDef1.setOptionsName(option);
			stringDef1.withCriteria("pop:low");

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef1);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(true, askQuery1.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef1);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			Assert.assertEquals(false, askQuery1.evaluate());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		} finally {
			clearData();
		}
	}

	@Test
	public void testQuerywithNoOptions() throws Exception {

		try {
			setupData();
			Thread.currentThread().sleep(2000L);
			StringQueryDefinition stringDef = qmgr.newStringDefinition();

			String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
			String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";

			MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(stringDef);
			Assert.assertEquals(true, askQuery.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef);
			MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(true, askQuery1.evaluate());

			StringQueryDefinition stringDef1 = qmgr.newStringDefinition();

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef1);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			Assert.assertEquals(true, askQuery1.evaluate());

			testAdminCon.setDefaultConstrainingQueryDefinition(stringDef1);
			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			Assert.assertEquals(true, askQuery1.evaluate());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		} finally {
			clearData();
		}
	}

	public void writeQueryOption(DatabaseClient client, String queryOptionName) throws FileNotFoundException {
		// create a manager for writing query options
		QueryOptionsManager optionsMgr = client.newServerConfigManager().newQueryOptionsManager();
		// create handle
		ReaderHandle handle = new ReaderHandle();
		// write the files
		BufferedReader docStream = new BufferedReader(new FileReader(
				MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX + queryOptionName).getFile()));
		handle.set(docStream);
		// handle.setFormat(Format.XML);
		// write the query options to the database
		optionsMgr.writeOptions(queryOptionName, handle);
	}

	// ISSUE 124
	@Test
	public void testStringQuery() throws Exception {

		try {
			setupData();
			StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");

			String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
			String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
			MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(stringDef);
			Assert.assertEquals(true, askQuery.evaluate());

			MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon
					.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			askQuery1.setConstrainingQueryDefinition(stringDef);
			Assert.assertEquals(false, askQuery1.evaluate());

			askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, posQuery);
			askQuery.setConstrainingQueryDefinition(null);
			Assert.assertEquals(true, askQuery.evaluate());

			askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, negQuery);
			askQuery1.setConstrainingQueryDefinition(null);
			Assert.assertEquals(true, askQuery1.evaluate());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		} finally {
			clearData();
		}
	}

	private void setupData() {
		DatabaseClient dbClient = DatabaseClientFactory.newClient(host, restPort, "admin", "admin",
				Authentication.DIGEST);
		String tripleDocOne = "<semantic-document>\n" + "<title>First Title</title>\n" + "<popularity>1</popularity>\n"
				+ "<size>100</size>\n" + "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">"
				+ "<sem:triple><sem:subject>http://example.org/r9928</sem:subject>"
				+ "<sem:predicate>http://example.org/p3</sem:predicate>"
				+ "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</sem:object></sem:triple>"
				+ "</sem:triples>\n" + "</semantic-document>";

		String tripleDocTwo = "<semantic-document>\n" + "<title>Second Title</title>\n" + "<popularity>5</popularity>\n"
				+ "<size>500</size>\n" + "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">"
				+ "<sem:triple><sem:subject>http://example.org/r9929</sem:subject>"
				+ "<sem:predicate>http://example.org/p3</sem:predicate>"
				+ "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</sem:object></sem:triple>"
				+ "</sem:triples>\n" + "</semantic-document>";

		DataMovementManager dmManager = dbClient.newDataMovementManager();
		WriteBatcher batcher = dmManager.newWriteBatcher();
		batcher.add("/directory1/doc1.xml", new StringHandle().with(tripleDocOne)).add("/directory2/doc2.xml",
				new StringHandle().with(tripleDocTwo));
		batcher.flushAndWait();
		dbClient.release();

	}

	private static void clearData() {
		DatabaseClient dbClient = DatabaseClientFactory.newClient(host, restPort, "admin", "admin",
				Authentication.DIGEST);
		XMLDocumentManager docMgr = dbClient.newXMLDocumentManager();
		docMgr.delete("/directory1/doc1.xml", "/directory2/doc2.xml");
		dbClient.release();
	}

	// ISSUE 51
	@Test
	public void testCommitConnClosed() throws Exception {
		try {
			testAdminCon.begin();
			testAdminCon.add(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(micah, fname, micahfname, dirgraph1);
			testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);
			Assert.assertEquals("Size of dirgraph1", 3, testAdminCon.size());
			testAdminCon.close();

		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected Exception");
		}
		// initializes repository and creates testAdminCon
		testAdminRepository.shutDown();
		testAdminRepository = null;
		testAdminCon = null;
		setUp();

		Assert.assertEquals("Size of dirgraph1", 0, testAdminCon.size());

	}

	@Test
	public void testHasStatement() throws Exception {
		try {
			BNode somenode = vf.createBNode();

			testAdminCon.add(somenode, fname, somenode, dirgraph);
			testAdminCon.add(somenode, lname, somenode, dirgraph1);
			testAdminCon.add(john, fname, somenode, (Resource) null);

			Assert.assertTrue(testAdminCon.hasStatement(john, null, null, false));
			Assert.assertTrue(testAdminCon.hasStatement(null, fname, null, false));
			Assert.assertTrue(testAdminCon.hasStatement(null, null, somenode, false));
			Assert.assertFalse(testAdminCon.hasStatement(micah, null, null, false));
			Assert.assertFalse(testAdminCon.hasStatement(john, fname, somenode, false, dirgraph));
			Assert.assertTrue(testAdminCon.hasStatement(somenode, fname, somenode, false, dirgraph));
			Assert.assertTrue(testAdminCon.hasStatement(somenode, null, null, false, dirgraph));
			Assert.assertTrue(testAdminCon.hasStatement(somenode, null, null, false, dirgraph1));
			Assert.assertTrue(testAdminCon.hasStatement(somenode, null, somenode, false, dirgraph1));
			Assert.assertFalse(testAdminCon.hasStatement(somenode, null, somenode, false, (Resource) null));

			Assert.assertTrue(testAdminCon.hasStatement(somenode, null, null, false));
			Assert.assertTrue(testAdminCon.hasStatement(john, fname, somenode, false, (Resource) null));
			Assert.assertTrue(testAdminCon.hasStatement(john, fname, somenode, true, (Resource) null));
			Assert.assertFalse(testAdminCon.hasStatement(john, fname, somenode, true, dirgraph));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		}
	}

	// RDF4J-25
	@Test
	public void testExportStatements() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph1);

		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(micah, lname, micahlname, (Resource) null);
		try {
			Assert.assertEquals(3, testAdminCon.size());

			testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					Assert.assertTrue(st.getContext().equals(dirgraph) || st.getContext().equals(dirgraph1));
					if (st.getContext().equals(dirgraph)) {
						assertThat(st, is((equalTo(st1))));
					} else if (st.getContext().equals(dirgraph1)) {
						Assert.assertEquals(new Double("111111111"),
								new Double(Double.parseDouble(st.getObject().stringValue())));
					} else {
						fail("Statement not returned");
					}
				}
			}, dirgraph, dirgraph1);

			testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph)) {
							assertThat(st, is((equalTo(st1))));
						} else if (st.getContext().equals(dirgraph1)) {
							Assert.assertEquals(new Double("111111111"),
									new Double(Double.parseDouble(st.getObject().stringValue())));
						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph, dirgraph1, null);

			testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph)) {
							assertThat(st, is((equalTo(st1))));
						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph, null);
			testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph1)) {
							Assert.assertEquals(new Double("111111111"),
									new Double(Double.parseDouble(st.getObject().stringValue())));

						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph1, null);
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("Unexpected exception");
			logger.error("Failed :", ex);
		}

		IRI temp = vf.createIRI("http://marklogic.com/temp/");
		testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				assertThat(st, is(equalTo((Resource) null)));
			}
		}, temp);
	}

	@Test
	public void testAddStatements() throws Exception {
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph1);
		Statement st3 = vf.createStatement(micah, lname, micahlname, dirgraph);

		testAdminCon.add(st1, dirgraph1);
		testAdminCon.add(st2, dirgraph);
		testAdminCon.add(st3, (Resource) null);
		try {
			Assert.assertEquals(3, testAdminCon.size());
			testWriterCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph)) {
							// Issue rdf4j-25
							Assert.assertEquals(new Double("111111111"),
									new Double(Double.parseDouble(st.getObject().stringValue())));

						} else if (st.getContext().equals(dirgraph1)) {
							assertThat(st, is((equalTo(vf.createStatement(john, fname, johnfname, dirgraph1)))));
						} else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph, dirgraph1, null);
		}

		catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed :", ex);
			ex.printStackTrace();
		}
	}

	@Test
	public void testNullContexts() throws Exception {

		Statement st = vf.createStatement(john, fname, johnfname, dirgraph);
		try {
			testAdminCon.add(st, null);
			fail("null context shouldn't be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		testAdminCon.add(st, (Resource) null);

		try {
			testAdminCon.getStatements(null, null, null, false, null);
			fail("null context shouldn't be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			testAdminCon.size(null);
			fail("null context shouldn't be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			testAdminCon.getStatements(null, null, null, true, null);
			fail("null context shouldn't be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			testAdminCon.exportStatements(null, null, null, false, new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {

				}
			}, null);
			fail("null context shouldn't be allowed");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		Assert.assertEquals(1, testAdminCon.size(new Resource[] {}));
		IRI temp = testAdminCon.getValueFactory().createIRI("http://marklogic.com/temp");
		Assert.assertEquals(0, testAdminCon.size(temp));
	}

	@Test
	public void testRemove() throws Exception {
		try {
			Statement st = vf.createStatement(john, fname, johnfname);
			Statement st1 = vf.createStatement(john, lname, johnlname);
			testAdminCon.add(st, dirgraph);
			testAdminCon.add(st1, (Resource) null);

			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size() == 2);

			testAdminCon.remove(st, (Resource) null);

			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size(null, null) == 1);
			Assert.assertTrue(testAdminCon.size() == 2);

			StatementList<Statement> sL1 = new StatementList<Statement>(st);
			sL1.add(st1);

			StatementIterator iter1 = new StatementIterator(sL1);
			Iterable<? extends Statement> iterable1 = new StatementIterable(iter1);
			Assert.assertTrue(iterable1.iterator().hasNext());

			testAdminCon.remove(iterable1);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(st, dirgraph);
			testAdminCon.add(st1, (Resource) null);

			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size() == 2);

			iter1 = new StatementIterator(sL1);
			iterable1 = new StatementIterable(iter1);

			testAdminCon.remove(iterable1, dirgraph);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 1);
			Assert.assertTrue(testAdminCon.size() == 1);

			testAdminCon.add(st, dirgraph);
			testAdminCon.add(st1, (Resource) null);

			Collection<Statement> c = Iterations.addAll(testAdminCon.getStatements(null, null, null, false),
					new ArrayList<Statement>());
			testAdminCon.remove(c);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			Statement st3 = vf.createStatement(micah, lname, micahlname, dirgraph1);

			testAdminCon.add(vf.createStatement(micah, lname, micahlname, dirgraph));
			testAdminCon.add(vf.createStatement(micah, lname, micahlname));

			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size((Resource) null) == 1);
			Assert.assertTrue(testAdminCon.size() == 2);

			testAdminCon.remove(st3);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size((Resource) null) == 1);

			testAdminCon.remove(st3, dirgraph);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size((Resource) null) == 1);

			testAdminCon.remove(st3, null, null);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size((Resource) null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(st3);
			sL1 = new StatementList<Statement>(st3);
			iter1 = new StatementIterator(sL1);
			iterable1 = new StatementIterable(iter1);
			testAdminCon.remove(iterable1);

			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(john, fname, johnfname, dirgraph);
			testAdminCon.add(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(st1, (Resource) null);

			sL1 = new StatementList<Statement>(st1);
			sL1.add(vf.createStatement(john, fname, johnfname, dirgraph));
			sL1.add(vf.createStatement(micah, lname, micahlname));

			iter1 = new StatementIterator(sL1);
			iterable1 = new StatementIterable(iter1);
			testAdminCon.remove(iterable1);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			sL1 = new StatementList<Statement>(st);
			sL1.add(st1);

			iter1 = new StatementIterator(sL1);
			Iteration<Statement, Exception> it = new IteratorIteration<Statement, Exception>(iter1);
			Assert.assertTrue(it.hasNext());

			testAdminCon.remove(it);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(st, dirgraph);
			testAdminCon.add(st1, (Resource) null);

			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size() == 2);

			iter1 = new StatementIterator(sL1);
			it = new IteratorIteration<Statement, Exception>(iter1);

			testAdminCon.remove(it, dirgraph);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 1);
			Assert.assertTrue(testAdminCon.size() == 1);

			testAdminCon.add(st, dirgraph);
			testAdminCon.add(st1, (Resource) null);

			testAdminCon.remove((Resource) null, null, (Value) null);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			st3 = vf.createStatement(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(st3);

			sL1 = new StatementList<Statement>(vf.createStatement(micah, lname, micahlname, dirgraph));
			iter1 = new StatementIterator(sL1);
			it = new IteratorIteration<Statement, Exception>(iter1);
			testAdminCon.remove(it);

			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(st1);
			testAdminCon.add(st, dirgraph);
			sL1 = new StatementList<Statement>(vf.createStatement(micah, lname, micahlname, dirgraph));
			sL1.add(st1);
			sL1.add(st);
			iter1 = new StatementIterator(sL1);
			it = new IteratorIteration<Statement, Exception>(iter1);
			testAdminCon.remove(it, (Resource) null);

			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 1);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 1);

			testAdminCon.remove(
					Iterations.addAll(testAdminCon.getStatements(null, null, null, false), new ArrayList<Statement>()));
			Assert.assertTrue(testAdminCon.size() == 0);

			st3 = vf.createStatement(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(st3);
			sL1 = new StatementList<Statement>(st3);
			iter1 = new StatementIterator(sL1);
			it = new IteratorIteration<Statement, Exception>(iter1);
			testAdminCon.remove(it);

			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);

			testAdminCon.add(john, fname, johnfname, dirgraph);
			testAdminCon.add(micah, lname, micahlname, dirgraph1);
			testAdminCon.add(st1, (Resource) null);

			sL1 = new StatementList<Statement>(st1);
			sL1.add(vf.createStatement(john, fname, johnfname, dirgraph));
			sL1.add(vf.createStatement(micah, lname, micahlname));

			iter1 = new StatementIterator(sL1);
			it = new IteratorIteration<Statement, Exception>(iter1);
			testAdminCon.remove(it);
			Assert.assertTrue(testAdminCon.size(dirgraph) == 0);
			Assert.assertTrue(testAdminCon.size(dirgraph1) == 0);
			Assert.assertTrue(testAdminCon.size(null, null) == 0);
			Assert.assertTrue(testAdminCon.size() == 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected Exception");
		} finally {
			if (testAdminCon.isActive())
				testAdminCon.rollback();
		}
	}

	@Test
	public void testRemoveDiffContexts() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);

		testAdminCon.add(st1, dirgraph1);
		testAdminCon.remove(st1);
		Assert.assertTrue(testAdminCon.hasStatement(st1, false, dirgraph1));
		Assert.assertFalse(testAdminCon.hasStatement(st1, false, dirgraph));

		testAdminCon.remove(vf.createStatement(john, fname, johnfname, dirgraph1));
		testAdminCon.remove(st1);
		Assert.assertFalse(testAdminCon.hasStatement(st1, false));
	}

	@Test
	public void testReadWriteDiffDB() throws Exception {
		DatabaseClient dbClient = DatabaseClientFactory.newClient(host, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
		MarkLogicRepository testDocRepository = new MarkLogicRepository(dbClient);
		testDocRepository.initialize();
		Assert.assertNotNull(testDocRepository);
		MarkLogicRepositoryConnection testDocCon = testDocRepository.getConnection();
		Assert.assertEquals(0, testDocCon.size());

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testDocCon.add(st1, dirgraph);
		testDocCon.add(st2, dirgraph);
		testDocCon.add(st3, dirgraph);
		testDocCon.add(st4, dirgraph);
		testDocCon.add(st5, dirgraph);
		testDocCon.add(st6, dirgraph);
		testDocCon.add(st7, dirgraph);
		testDocCon.add(st8, dirgraph);
		testDocCon.add(st9, dirgraph);
		testDocCon.add(st10, dirgraph);

		Assert.assertEquals(10, testDocCon.size(dirgraph));

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append("         SELECT DISTINCT ?person");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person ad:firstName ?firstname ;");
		queryBuilder.append(" ad:lastName ?lastname.");
		queryBuilder.append(" OPTIONAL {?person ad:homeTel ?phonenumber .}");
		queryBuilder.append(" FILTER (?firstname = \"Fei\")}");

		TupleQuery query = testDocCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		TupleQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				Value nameResult = solution.getValue("person");
				Assert.assertEquals(nameResult.stringValue(), fei.stringValue());
			}
		} finally {
			result.close();
		}
		testDocCon.clear();
		Assert.assertEquals(0, testDocCon.size(dirgraph));
		Assert.assertEquals(0, testDocCon.size());

		testDocCon.close();
		testDocRepository.shutDown();
		dbClient.release();
		testDocRepository = null;
		testDocCon = null;
	}

	@Test
	public void testTransactionAccess() {
		Assert.assertNull(testWriterCon.getTransaction());
		testWriterCon.begin();
		Assert.assertNotNull(testWriterCon.getTransaction());
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);

		testWriterCon.add(st1);
		testWriterCon.add(st2);
		testWriterCon.add(st3);
		testWriterCon.add(st4);
		testWriterCon.add(st5);
		testWriterCon.add(st6);

		File file = new File("src/test/resources/testdata/dir.xml");
		DocumentManager docMgr = databaseClient.newDocumentManager();
		String docId = "/tx-rollback/bbq1.xml";
		FileHandle handle = new FileHandle(file);
		handle.set(file);
		handle.setFormat(Format.XML);
		docMgr.write(docId, handle, testWriterCon.getTransaction());
		DocumentDescriptor documentDescriptor = docMgr.exists(docId, testWriterCon.getTransaction());
		testWriterCon.add(st7);
		testWriterCon.add(st8);
		testWriterCon.add(st9);
		testWriterCon.add(st10);

		Assert.assertEquals(docId, documentDescriptor.getUri());
		Assert.assertNull(docMgr.exists(docId));
		Assert.assertEquals(10, testWriterCon.size());
		testWriterCon.commit();
		Assert.assertEquals(10, testWriterCon.size());
		Assert.assertNull(testWriterCon.getTransaction());
		Assert.assertNotNull(docMgr.exists(docId));
	}

	@Test
	public void testExport() throws Exception {

		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph1);

		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(micah, lname, micahlname, (Resource) null);
		try {
			Assert.assertEquals(3, testAdminCon.size());

			testAdminCon.export(new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					Assert.assertTrue(st.getContext().equals(dirgraph) || st.getContext().equals(dirgraph1));
					if (st.getContext().equals(dirgraph)) {
						assertThat(st, is((equalTo(st1))));
					} else if (st.getContext().equals(dirgraph1)) {
						Assert.assertEquals(new Double("111111111"),
								new Double(Double.parseDouble(st.getObject().stringValue())));
					} else {
						fail("Statement not returned");
					}
				}
			}, dirgraph, dirgraph1);

			testAdminCon.export(new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph)) {
							assertThat(st, is((equalTo(st1))));
						} else if (st.getContext().equals(dirgraph1)) {
							Assert.assertEquals(new Double("111111111"),
									new Double(Double.parseDouble(st.getObject().stringValue())));
						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph, dirgraph1, null);

			testAdminCon.export(new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph)) {
							assertThat(st, is((equalTo(st1))));
						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph, null);
			testAdminCon.export(new AbstractRDFHandler() {
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (st.getContext() == null) {
						assertThat(st, is((equalTo(vf.createStatement(micah, lname, micahlname, null)))));
					} else {
						if (st.getContext().equals(dirgraph1)) {
							Assert.assertEquals(new Double("111111111"),
									new Double(Double.parseDouble(st.getObject().stringValue())));

						}

						else {
							fail("Statement not returned");
						}
					}
				}
			}, dirgraph1, null);
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("Unexpected exception");
			logger.error("Failed :", ex);
		}

		IRI temp = vf.createIRI("http://marklogic.com/temp/");
		testAdminCon.export(new AbstractRDFHandler() {
			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				assertThat(st, is(equalTo((Resource) null)));
			}
		}, temp);
	}
}
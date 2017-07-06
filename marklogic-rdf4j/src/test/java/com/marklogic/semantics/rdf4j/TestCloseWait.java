package com.marklogic.semantics.rdf4j;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class TestCloseWait extends Rdf4jTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected ValueFactory vf;

    protected URI dirgraph;

    private static final String ID = "id";
    private static final String ADDRESS = "addressbook";
    protected static final String NS = "http://marklogicsparql.com/";

    protected static MarkLogicRepository rep;
    protected static MarkLogicRepositoryConnection conn;
    protected static DatabaseClient dbClient;

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");
        dbClient = DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.DigestAuthContext(adminUser, adminPassword));
        rep =  new MarkLogicRepository(dbClient);
        rep.initialize();
        vf = rep.getValueFactory();
        conn =rep.getConnection();
        logger.info("test setup complete.");
    }

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        if( conn.isOpen() && conn.isActive()){conn.rollback();}
        if(conn.isOpen()){conn.clear();}
        conn.close();
        rep.shutDown();
        conn=null;
        rep = null;
        logger.info("looking for CLOSE_WAIT.");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testMultiThreadedAdd2() throws Exception{

        Assert.assertEquals(0, conn.size());
        class MyRunnable implements Runnable {
            @Override
            public void run(){
                try {
                    for (int j =0 ;j < 100; j++){
                        URI subject = vf.createURI(NS+ID+"/"+Thread.currentThread().getId()+"/"+j+"#1111");
                        URI predicate = vf.createURI(NS+ADDRESS+"/"+Thread.currentThread().getId()+"/"+"#firstName");
                        Literal object = vf.createLiteral(Thread.currentThread().getId()+ "-" + j +"-" +"John");
                        conn.add(subject, predicate,object, dirgraph);
                    }
                } catch (RepositoryException e1) {
                    e1.printStackTrace();
                }finally {
                    try {
                        conn.sync();
                    } catch (MarkLogicRdf4jException e) {
                        e.printStackTrace();
                    }
                }

            }
        }

        Thread t1,t2,t3,t4;
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

        Assert.assertEquals(400, conn.size());
    }
}
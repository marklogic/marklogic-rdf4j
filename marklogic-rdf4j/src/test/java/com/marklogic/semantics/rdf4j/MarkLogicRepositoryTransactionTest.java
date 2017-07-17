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
 * Sesame API.
 */
package com.marklogic.semantics.rdf4j;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * test transactions
 *
 * @author James Fuller
 */
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MarkLogicRepositoryTransactionTest extends Rdf4jTestBase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
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
        conn = null;
        rep.shutDown();
        rep = null;
        logger.info("tearDown complete.");
    }


    @Test
    public void testSizeCommit()
            throws Exception
    {
        writerRep.initialize();

        MarkLogicRepositoryConnection other= writerRep.getConnection();

        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");

        ValueFactory vf= conn.getValueFactory();
        IRI fei = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI lname = vf.createIRI("http://marklogicsparql.com/addressbook#lastName");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        Literal feilname = vf.createLiteral("Ling", "nonexistentlangtag");
        Literal feiage = vf.createLiteral(25);

        conn.setIsolationLevel(IsolationLevels.SNAPSHOT);
        assertThat(other.size(), is(equalTo(0L)));
        assertThat(conn.size(), is(equalTo(0L)));
        try {
            conn.begin();
            conn.add(fei, age, feiage,context5);
            assertThat(conn.size(), is(equalTo(1L)));
            assertThat(other.size(), is(equalTo(0L)));
            conn.add(fei, lname,feilname);
            assertThat(conn.size(), is(equalTo(2L)));
            assertThat(other.size(), is(equalTo(0L)));
            conn.commit();
        }
        catch (Error err){
            err.printStackTrace();
            throw (err);
        }finally{
            if (conn.isActive())
                logger.info("active transaction");
        }
        assertThat(conn.size(), is(equalTo(2L)));
        assertThat(other.size(), is(equalTo(2L)));
    }

    @Test
    public void testSizeSimpleCommit()
            throws Exception
    {

        Resource context5 = conn.getValueFactory().createIRI("http://marklogic.com/test/context5");

        ValueFactory vf= conn.getValueFactory();
        IRI fei = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        Literal feiage = vf.createLiteral(25);

        conn.setIsolationLevel(IsolationLevels.SNAPSHOT);
        assertThat(conn.size(), is(equalTo(0L)));
        conn.begin();
        conn.add(fei, age, feiage, context5);
        assertThat(conn.size(), is(equalTo(1L)));
        conn.commit();
        assertThat(conn.size(), is(equalTo(1L)));
    }

    @Test
    public void testMultipleCommit()
            throws Exception
    {

        Resource context1 = conn.getValueFactory().createIRI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createIRI("http://marklogic.com/test/context2");

        ValueFactory vf= conn.getValueFactory();
        IRI gary = vf.createIRI("http://marklogicsparql.com/id#3333");
        IRI age = vf.createIRI("http://marklogicsparql.com/addressbook#age");
        IRI gender = vf.createIRI("http://marklogicsparql.com/addressbook#gender");

        Literal garyage = vf.createLiteral(25);
        Literal garygender = vf.createLiteral("male");

        assertEquals(conn.size(), 0L);
        conn.begin();
        conn.add(gary, age, garyage, context1);
        conn.add(gary, gender, garygender, context1);
        conn.add(gary, age, garyage);
        assertEquals(conn.size(), 3L);
        conn.commit();

        try {
            conn.begin();
            conn.add(gary, age, garyage, context2);
            assertEquals("expected 4 here",conn.size(), 4L);
            conn.commit();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(conn.isActive())
                conn.rollback();
        }
        assertEquals(conn.size(), 4L);

        CloseableIteration<? extends Statement, RepositoryException> iter = conn.getStatements(null, null,
                null, false, null , context1);

        int count = 0;
        while (iter.hasNext()) {
            count++;
            Statement st = iter.next();
            System.out.println("Context is "+st.getContext());
            assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource)context1)) ));
        }
        assertEquals("there should be three statements", 3, count);

        assertEquals("expected 4",conn.size(), 4L);
    }

}

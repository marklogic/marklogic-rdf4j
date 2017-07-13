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
package com.marklogic.semantics.rdf4j.query;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.Rdf4jTestBase;

/**
 * test TupleQuery
 *
 * @author James Fuller
 */
public class MarkLogicTupleQueryTest extends Rdf4jTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp() throws RepositoryException, FileNotFoundException {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");
        File testData = new File(TESTFILE_OWL);

        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);
        gmgr.write("/directory1/test.rdf", new FileHandle(testData));
    }

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        if(conn != null){conn.close();}
        conn = null;
        if(rep != null){rep.shutDown();}
        rep = null;
        logger.info("tearDown complete.");
        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.delete("/directory1/test.rdf");
    }

    @Test
    public void testSPARQLQueryWithPrepareQuery()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 1 ";
        Query q = conn.prepareQuery(QueryLanguage.SPARQL, queryString);

        try {
            if (q instanceof TupleQuery) {
                TupleQueryResult result = ((TupleQuery) q).evaluate();
                while (result.hasNext()) {
                    BindingSet tuple = result.next();
                    Assert.assertEquals("s", tuple.getBinding("s").getName());
                    Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", tuple.getBinding("s").getValue().stringValue());
                }
                result.close();
            }
        }
        catch(Exception ex)
        {
            throw ex;
        }
        finally {
            conn.close();
            Thread.sleep(1000);
        }

    }

    @Test
    public void testSPARQLQuery()
            throws Exception {
        try {
            for (int i=0; i<101;i++){
                String queryString = "select * { ?s ?p ?o } limit 2 ";
                TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                TupleQueryResult results = tupleQuery.evaluate();

                Assert.assertEquals(results.getBindingNames().get(0), "s");
                Assert.assertEquals(results.getBindingNames().get(1), "p");
                Assert.assertEquals(results.getBindingNames().get(2), "o");

                BindingSet bindingSet = results.next();

                Value sV = bindingSet.getValue("s");
                Value pV = bindingSet.getValue("p");
                Value oV = bindingSet.getValue("o");

                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
                Assert.assertEquals("0", oV.stringValue());

                BindingSet bindingSet1 = results.next();

                Value sV1 = bindingSet1.getValue("s");
                Value pV1 = bindingSet1.getValue("p");
                Value oV1 = bindingSet1.getValue("o");

                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BabylonGeodata", sV1.stringValue());
                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
                Assert.assertEquals("0", oV1.stringValue());
                results.close();
            }
        }
        catch(Exception ex)
        {
            throw ex;
        }
        finally {
            conn.close();
            Thread.sleep(1000);
        }
    }

    @Test
    public void testPrepareTupleQueryQueryStringMethod() throws Exception{
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 10 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(queryString);
        tupleQuery = conn.prepareTupleQuery(queryString,"http://marklogic.com/test/baseuri");
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");
        results.close();
    }

    @Test
    public void testSPARQLQueryWithDefaultInferred()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleQuery.setIncludeInferred(true);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BabylonGeodata", sV1.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
        Assert.assertEquals("0", oV1.stringValue());
        results.close();
    }

    @Test
    public void testSPARQLQueryDistinct()
            throws Exception {

        try {
            String queryString = "SELECT DISTINCT ?_ WHERE { GRAPH ?ctx { ?s ?p ?o . } }";
            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            TupleQueryResult result = tupleQuery.evaluate();
            RepositoryResult<Resource> rr =
                    new RepositoryResult<Resource>(
                            new ExceptionConvertingIteration<Resource, RepositoryException>(
                                    new ConvertingIteration<BindingSet, Resource, QueryEvaluationException>(result) {

                                        @Override
                                        protected Resource convert(BindingSet bindings)
                                                throws QueryEvaluationException {
                                            return (Resource) bindings.getValue("_");
                                        }
                                    }) {

                                @Override
                                protected RepositoryException convert(Exception e) {
                                    return new RepositoryException(e);
                                }
                            });

            Assert.assertTrue(rr.hasNext()); //Resource resource = rr.next();

            //logger.debug(resource.stringValue());
            result.close();

        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    @Test
    public void testSPARQLQueryWithPagination()
            throws Exception {
        try{
            for(int i=0; i<101; i++){
                String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
                MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                TupleQueryResult results = tupleQuery.evaluate(3, 1);

                Assert.assertEquals(results.getBindingNames().get(0), "s");
                Assert.assertEquals(results.getBindingNames().get(1), "p");
                Assert.assertEquals(results.getBindingNames().get(2), "o");

                BindingSet bindingSet = results.next();

                Value sV = bindingSet.getValue("s");
                Value pV = bindingSet.getValue("p");
                Value oV = bindingSet.getValue("o");

                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BethanyBeyondtheJordanGeodata", sV.stringValue());
                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
                Assert.assertEquals("0", oV.stringValue());

                Assert.assertFalse(results.hasNext());
                results.close();
            }
        }
        catch(Exception ex)
        {
            throw ex;
        }
        finally {
            conn.close();
            Thread.sleep(1000);
        }
    }

    //https://bugtrack.marklogic.com/41543
    @Test
    public void testSPARQLQueryCloseWait()
            throws Exception {
        try{
            String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100";
            MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            for(int i=0; i<101; i++){

                TupleQueryResult results = tupleQuery.evaluate(3,1);
                // must close query result
                results.close();
            }

        }
        catch(Exception ex)
        {
            throw ex;
        }
        finally {
            conn.close();
            Thread.sleep(1000);
        }
    }

    @Test
    public void testSPARQLQueryWithRuleset()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setRulesets(SPARQLRuleset.RDFS_FULL);
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());
        results.close();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/111
    @Test
    public void testSPARQLQueryWithMultipleRulesets()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setRulesets(SPARQLRuleset.RDFS_FULL,SPARQLRuleset.DOMAIN);
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());
        results.close();
    }

    @Test
    public void testSPARQLQueryWithResultsHandler()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 10";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.evaluate(new TupleQueryResultHandler() {
            @Override
            public void startQueryResult(List<String> bindingNames) {
                Assert.assertEquals(bindingNames.get(0), "s");
                Assert.assertEquals(bindingNames.get(1), "p");
                Assert.assertEquals(bindingNames.get(2), "o");
            }

            @Override
            public void handleSolution(BindingSet bindingSet) {
                Assert.assertEquals(bindingSet.getBinding("o").getValue().stringValue(), "0");
            }

            @Override
            public void endQueryResult() {
            }

            @Override
            public void handleBoolean(boolean arg0)
                    throws QueryResultHandlerException {
            }

            @Override
            public void handleLinks(List<String> arg0)
                    throws QueryResultHandlerException {
            }
        });
        //tupleQuery.evaluate();
    }

    @Test
    public void testSPARQLQueryBindings()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o . filter (?s = ?b) filter (?p = ?c) }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jim"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        tupleQuery.removeBinding("c");

        // TBD -  Assert. for confirmation of removal

        Assert.assertEquals(null, tupleQuery.getBindings().getBinding("c"));

        tupleQuery.clearBindings();

        Assert.assertEquals(null, tupleQuery.getBindings().getBinding("b"));

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jotham"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        logger.info(results.getBindingNames().toString());

        Assert.assertTrue(results.hasNext());
        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Jotham", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#parentOf", pV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Ahaz", oV.stringValue());
        results.close();
    }


    @Test
    public void testSPARQLWithWriter()
            throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(out);
        sparqlWriter.getWriterConfig().set(BasicWriterSettings.PRETTY_PRINT, true);

        String expected = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<sparql xmlns='http://www.w3.org/2005/sparql-results#'>\n" +
                "\t<head>\n" +
                "\t\t<variable name='s'/>\n" +
                "\t\t<variable name='p'/>\n" +
                "\t\t<variable name='o'/>\n" +
                "\t</head>\n" +
                "\t<results>\n" +
                "\t\t<result>\n" +
                "\t\t\t<binding name='s'>\n" +
                "\t\t\t\t<uri>http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata</uri>\n" +
                "\t\t\t</binding>\n" +
                "\t\t\t<binding name='p'>\n" +
                "\t\t\t\t<uri>http://semanticbible.org/ns/2006/NTNames#altitude</uri>\n" +
                "\t\t\t</binding>\n" +
                "\t\t\t<binding name='o'>\n" +
                "\t\t\t\t<literal datatype='http://www.w3.org/2001/XMLSchema#int'>0</literal>\n" +
                "\t\t\t</binding>\n" +
                "\t\t</result>\n" +
                "\t</results>\n" +
                "</sparql>\n";

        String queryString = "select * { ?s ?p ?o . } limit 1";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.evaluate(sparqlWriter);
        Assert.assertEquals(expected, out.toString());

    }

    @Test
    public void testSPARQLQueryWithEmptyResults()
            throws Exception {
        String queryString = "select * { <http://marklogic.com/nonexistent> ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertFalse(results.hasNext());
        results.close();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/163
    @Test
    public void testSPARQLQueryWithNullRulesets()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setRulesets(SPARQLRuleset.RDFS_FULL,null);
        tupleQuery.setRulesets(null);
        Assert.assertTrue(tupleQuery.getRulesets() == null);

        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        results.close();
    }

    @Test(expected=org.eclipse.rdf4j.query.QueryEvaluationException.class)
    public void testSPARQLQueryQueryEvaluationException()
            throws Exception {
        String queryString = "select *  <http://marklogic.com/nonexistent> ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertFalse(results.hasNext());
        results.close();
    }

    @Test(expected=org.eclipse.rdf4j.query.QueryEvaluationException.class)
    public void testSPARQLMalformedException()
            throws Exception {
        String queryString = "select1 *  <http://marklogic.com/nonexistent> ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertFalse(results.hasNext());
        results.close();
    }
}
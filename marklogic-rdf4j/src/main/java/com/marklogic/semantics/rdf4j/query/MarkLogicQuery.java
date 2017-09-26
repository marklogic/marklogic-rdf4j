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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.rdf4j.MarkLogicRdf4jException;
import com.marklogic.semantics.rdf4j.client.MarkLogicClient;
import com.marklogic.semantics.rdf4j.client.MarkLogicClientDependent;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.impl.AbstractQuery;
import org.eclipse.rdf4j.repository.sparql.query.QueryStringUtil;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base query class
 *
 * @author James Fuller
 */
public class MarkLogicQuery extends AbstractQuery implements Query,MarkLogicClientDependent,MarkLogicQueryDependent {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicQuery.class);

    private MarkLogicClient client;

    private String queryString;

    private String baseURI;

    private SPARQLQueryBindingSet bindingSet;

    private boolean includeInferred;

    /**
     * Constructor
     *
     * @param client
     * @param bindingSet
     * @param baseUri
     * @param queryString
     */
    public MarkLogicQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString, GraphPermissions graphPerms, QueryDefinition defaultQueryDef, SPARQLRuleset[] rulesets) {
        super();
        setBaseURI(baseUri);
        setQueryString(queryString);
        setMarkLogicClient(client);
        setBindings(bindingSet);
        setIncludeInferred(true); // is default set true
        setGraphPerms(graphPerms);
        setConstrainingQueryDefinition(defaultQueryDef);
        setRulesets(rulesets);
    }

    /**
     * sets MarkLogicClient
     *
     * @param client
     */
    @Override
    public void setMarkLogicClient(MarkLogicClient client) {
        this.client=client;
    }

    /**
     * get MarkLogicClient
     *
     * @return
     */
    @Override
    public MarkLogicClient getMarkLogicClient() {
        return this.client;
    }

    /**
     * sets the query string
     *
     * @return
     */
    public String getQueryString() {
        return QueryStringUtil.getQueryString(this.queryString, getBindings());
    }

    /**
     * sets the query string
     *
     * @param queryString
     */
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    /**
     * sets bindings used by query
     *
     * @param bindingSet
     */
    public void setBindings(SPARQLQueryBindingSet bindingSet) {
        this.bindingSet=bindingSet;
    }

    /**
     * gets bindings used by query
     *
     * @return
     */
    @Override
    public SPARQLQueryBindingSet getBindings() {
        return this.bindingSet;
    }

    /**
     * Set individual binding.
     *
     * @param name
     * @param stringValue
     */
    public void setBinding(String name, String stringValue) {
        bindingSet.addBinding(name, SimpleValueFactory.getInstance().createIRI(stringValue));
    }

    /**
     * Set individual binding and value.
     * @param name
     * @param value
     */
    @Override
    public void setBinding(String name, Value value) {
        bindingSet.addBinding(name, value);
    }

    /**
     * Remove binding.
     *
     * @param name
     */
    @Override
    public void removeBinding(String name) {
        bindingSet.removeBinding(name);
    }

    /**
     * Clear bindings.
     *
     */
    @Override
    public void clearBindings() {
        bindingSet.removeAll(bindingSet.getBindingNames());
    }

    /**
     * Set true or false to use default inference ruleset.
     *
     * @param includeInferred
     */
    @Override
    public void setIncludeInferred(boolean includeInferred) {
        this.includeInferred=includeInferred;
    }

    /**
     * Return true or false if using default inference ruleset.
     *
     * @return boolean
     */
    @Override
    public boolean getIncludeInferred() {
        return this.includeInferred;
    }

    /**
     * Implemented to honor interface.
     *
     * @param dataset
     */
    @Override
    public void setDataset(Dataset dataset) {
    }

    /**
     * Implemented to honor interface.
     *
     * @return
     */
    @Override
    public Dataset getDataset() {
        return null;
    }

    /**
     * Sets maximum execution time for query.
     *
     * @param maxExecTime
     */
    @Override
    public void setMaxExecutionTime(int maxExecTime) {
    }

    /**
     * getter for MaxExecutionTime
     * @return int
     */
    @Override
    public int getMaxExecutionTime() {
        return 0;
    }

    /**
     * getter for BaseURI.
     * @return
     */
    @Override
    public String getBaseURI() {
        return baseURI;
    }

    /**
     * setter for BaseURI.
     * @param baseURI
     */
    @Override
    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    /**
     * Sets constraining query.
     *
     * @param constrainingQueryDefinition
     */
    @Override
    public void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition) {
        getMarkLogicClient().setConstrainingQueryDefinition(constrainingQueryDefinition);
    }

    /**
     * getter for ConstrainingQueryDefinition.
     * @return
     */
    @Override
    public QueryDefinition getConstrainingQueryDefinition() {
        return getMarkLogicClient().getConstrainingQueryDefinition();
    }

    /**
     * Sets the inference rulesets to be used by query.
     *
     * @param ruleset
     */
    public void setRulesets(SPARQLRuleset ... ruleset){
        getMarkLogicClient().setRulesets(ruleset);
    }

    /**
     * Getter for rulesets.
     * @return
     */
    public SPARQLRuleset[] getRulesets(){
        return getMarkLogicClient().getRulesets();
    }

    /**
     * Sets the graph permissions to be used by query.
     *
     * @param graphPerms
     */
    @Override
    public void setGraphPerms(GraphPermissions graphPerms) {
        getMarkLogicClient().setGraphPerms(graphPerms);
    }

    /**
     * Sets the graph permissions to be used by query.
     * @return
     */
    @Override
    public GraphPermissions getGraphPerms() {
        return getMarkLogicClient().getGraphPerms();
    }

    protected void sync() throws MarkLogicRdf4jException {
        getMarkLogicClient().sync();
    }
}

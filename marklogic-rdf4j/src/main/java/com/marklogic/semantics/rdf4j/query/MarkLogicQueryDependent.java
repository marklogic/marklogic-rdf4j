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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;

/**
 * interface defining MarkLogic semantic features
 *
 * @author James Fuller
 */
interface MarkLogicQueryDependent {

    SPARQLRuleset[] getRulesets();
    void setRulesets(SPARQLRuleset ... ruleset);

    String getBaseURI();
    void setBaseURI(String baseURI);

    QueryDefinition getConstrainingQueryDefinition();
    void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition);

    GraphPermissions getGraphPerms();
    void setGraphPerms(GraphPermissions graphPerms);

}

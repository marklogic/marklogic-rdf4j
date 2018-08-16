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
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * RDF4J API.
 */
package com.marklogic.semantics.rdf4j;

import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Specific exception for throwing MarkLogic related errors.
 *
 *
 */
@SuppressWarnings("serial")
public class MarkLogicRdf4jException extends RepositoryException {

    public MarkLogicRdf4jException(String message) {
        super(message);
    }

    public MarkLogicRdf4jException(Exception e) {
        super(e);
    }

    public MarkLogicRdf4jException(String message, Exception e) {
        super(message,e);
    }
}
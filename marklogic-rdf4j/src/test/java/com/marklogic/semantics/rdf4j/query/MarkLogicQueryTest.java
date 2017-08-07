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
package com.marklogic.semantics.rdf4j.query;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import org.junit.*;

import java.io.*;

import static com.marklogic.semantics.rdf4j.Rdf4jTestBase.*;

// minimal java api client test which illustrates CLOSE_WAIT

public class MarkLogicQueryTest  {

    public static DatabaseClient writerClient;

    protected static final String TESTFILE_OWL = "src/test/resources/testdata/test-small.owl";

    @Before
    public void setUp() throws FileNotFoundException {
        writerClient = DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.DigestAuthContext(user, password));
    }

    @Test
    public void testCloseWait()
            throws Exception {
        try {
            File testData = new File(TESTFILE_OWL);
            FileHandle testHandle = new FileHandle(testData);
            GraphManager gmgr = writerClient.newGraphManager();
            gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);
            gmgr.merge("/directory1/test.rdf", testHandle);
            gmgr.delete("/directory1/test.rdf");
        } catch (Exception ex) {
            throw ex;
        } finally {
            writerClient.release();
            // increase Thread.sleep or set breakpoints on gmgr.merge and gmgr.delete to observe CLOSE_WAIT
            Thread.sleep(100);
        }
    }
}

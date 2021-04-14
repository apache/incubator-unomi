/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.unomi.shell.migration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.karaf.shell.api.console.Session;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Version;

import java.io.IOException;

/**
 * This interface must be implemented if you create a new migration class
 * @author dgaillard
 */
public interface Migration {
    /**
     * This method return the minimal version before applying this migration
     * TODO: not used for now
     * @return return the version
     */
    Version getFromVersion();

    /**
     * This method return the target version after migration
     * @return the target version
     */
    Version getToVersion();

    /**
     * This method returns a short description of the changes performed during this migration
     * @return the migration description
     */
    String getDescription();

    /**
     * This method is called to execute the migration
     * @param session       the shell's session
     * @param httpClient    CloseableHttpClient
     * @param esAddress     the fully qualified address at which the ElasticSearch is reachable (ie http://localhost:9200)
     * @param bundleContext the bundle context object
     * @throws IOException if there was an error while executing the migration
     */
    void execute(Session session, CloseableHttpClient httpClient, String esAddress, BundleContext bundleContext) throws IOException;
}

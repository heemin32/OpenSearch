/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import junit.framework.TestCase;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;

import static org.mockito.Mockito.mock;

public class DatasourceDatabaseServiceTests extends TestCase {
    public void testCreateDocument() {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        DatasourceDatabaseService databaseService = new DatasourceDatabaseService(client, clusterService);
        String[] names = {"ip", "country", "city"};
        String[] values = {"1.0.0.0/25", "USA", "Seattle"};
        assertEquals("{\"ip\":\"1.0.0.0/25\",\"data\":{\"country\":\"USA\",\"city\":\"Seattle\"}}", databaseService.createDocument(names, values));
    }
}

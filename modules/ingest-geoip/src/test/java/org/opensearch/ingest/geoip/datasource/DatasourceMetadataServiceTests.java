/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ingest.geoip.datasource.common.DatasourceMetadata;
import org.opensearch.ingest.geoip.datasource.common.DatasourceState;
import org.opensearch.test.rest.RestActionTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME;

public class DatasourceMetadataServiceTests extends RestActionTestCase {

    public void testAddDatasourceIfAbsent() throws Exception {
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setEndpoint("https://test.com");
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        verifyingClient.setExecuteVerifier((actionRequest, actionResponse) -> {
            assertTrue(actionResponse instanceof IndexRequest);
            IndexRequest indexRequest = (IndexRequest) actionResponse;
            assertEquals(GEOIP_DATASOURCE_INDEX_NAME, indexRequest.index());
            assertEquals("test", indexRequest.id());
            return null;
        });

        ClusterService clusterService = mock(ClusterService.class);
        DatasourceMetadataService service = new DatasourceMetadataService(verifyingClient, clusterService);
        service.addDatasourceIfAbsent(request, mock(ActionListener.class));
    }

    public void testCreateDatasourceIndexIfAbsent() {
        CreateIndexResponse createIndexResponse = new CreateIndexResponse(true, true, "test");

        ActionFuture<CreateIndexResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(createIndexResponse);

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.create(any())).thenReturn(actionFuture);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ClusterService clusterService = mock(ClusterService.class);
        DatasourceMetadataService service = new DatasourceMetadataService(client, clusterService);
        service.createDatasourceIndexIfAbsent();

        ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(indicesAdminClient).create(captor.capture());
        assertEquals(GEOIP_DATASOURCE_INDEX_NAME, captor.getValue().index());
        assertEquals(1, (int)captor.getValue().settings().getAsInt("index.number_of_shards", 0));
        assertEquals("0-all", captor.getValue().settings().get("index.auto_expand_replicas"));
    }
}

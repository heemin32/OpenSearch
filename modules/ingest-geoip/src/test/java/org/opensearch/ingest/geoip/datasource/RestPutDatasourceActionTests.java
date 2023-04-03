/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.junit.Before;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

public class RestPutDatasourceActionTests extends RestActionTestCase {

    private RestPutDatasourceAction action;

    @Before
    public void setupAction() {
        action = new RestPutDatasourceAction();
        controller().registerHandler(action);
    }

    public void testPrepareRequest() throws Exception {
        String content = "{\"endpoint\":\"https://test.com\", \"update_interval\":1}";
        RestRequest restRequest = new FakeRestRequest.Builder(
            xContentRegistry())
            .withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://test.com", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(1), putDatasourceRequest.getUpdateInterval());
            assertEquals("test", putDatasourceRequest.getId());
            return null;
        });

        dispatchRequest(restRequest);
    }

    public void testPrepareRequestDefaultValue() throws Exception {
        RestRequest restRequestWithEmptyContent = new FakeRestRequest.Builder(
            xContentRegistry())
            .withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        RestRequest restRequestWithoutContent = new FakeRestRequest.Builder(
            xContentRegistry())
            .withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .build();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://geoip.maps.opensearch.org/v1/geolite-2/manifest.json", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(3), putDatasourceRequest.getUpdateInterval());
            assertEquals("test", putDatasourceRequest.getId());
            return null;
        });

        dispatchRequest(restRequestWithEmptyContent);
        dispatchRequest(restRequestWithoutContent);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ingest.geoip.datasource.common.DatasourceSettings;
import org.opensearch.ingest.geoip.datasource.PutDatasourceAction;
import org.opensearch.ingest.geoip.datasource.PutDatasourceRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest handler for GeoIP datasource creation
 */
public class RestPutDatasourceAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "ingest_processor_geoip_datasource";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final PutDatasourceRequest putDatasourceRequest = new PutDatasourceRequest(request.param("id"));
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                PutDatasourceRequest.PARSER.parse(parser, putDatasourceRequest, null);
            }
        }
        if (putDatasourceRequest.getEndpoint() == null) {
            putDatasourceRequest.setEndpoint(DatasourceSettings.DATASOURCE_ENDPOINT.get(client.settings()));
        }
        if (putDatasourceRequest.getUpdateInterval() == null) {
            putDatasourceRequest.setUpdateInterval(DatasourceSettings.DATASOURCE_UPDATE_INTERVAL.get(client.settings()));
        }
        return channel -> client.execute(PutDatasourceAction.INSTANCE, putDatasourceRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        // TODO enable when feature implementation is completed
        boolean enabled = false;
        return unmodifiableList(enabled ? asList(new Route(PUT, "/_geoip/datasource/{id}")) : asList());
    }
}

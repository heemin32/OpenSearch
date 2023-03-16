/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.put;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ingest.geoip.datasource.DatasourceSettings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestPutDatasourceAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "ingest_processor_geoip_datasource";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutDatasourceRequest putDatasourceRequest;
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                putDatasourceRequest = PutDatasourceRequest.PARSER.parse(parser, new PutDatasourceRequest(), null);
                if (putDatasourceRequest.getEndpoint() == null) {
                    putDatasourceRequest.setEndpoint(DatasourceSettings.DATASOURCE_ENDPOINT.get(client.settings()));
                }
                if (putDatasourceRequest.getUpdateInterval() == null) {
                    putDatasourceRequest.setUpdateInterval(DatasourceSettings.DATASOURCE_UPDATE_INTERVAL.get(client.settings()));
                }
            }
        } else {
            putDatasourceRequest = new PutDatasourceRequest();
        }
        putDatasourceRequest.setId(request.param("name"));
        return channel -> client.execute(PutDatasourceAction.INSTANCE, putDatasourceRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(PUT, "/_geoip/datasource/{name}")));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class PutDatasourceRequestTests extends OpenSearchTestCase {

    public void testValidateInvalidUrl() {
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setEndpoint("invalidUrl");
        request.setUpdateInterval(TimeValue.ZERO);
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Invalid URL format is provided", exception.validationErrors().get(0));
    }

    public void testValidateInvalidManifestFile() {
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setId("test");
        request.setEndpoint("https://hi.com");
        request.setUpdateInterval(TimeValue.ZERO);
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Failed to parse the manifest file", exception.validationErrors().get(0));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.common;

import org.opensearch.test.OpenSearchTestCase;

public class DatasourceSettingsTests extends OpenSearchTestCase {

    public void testValidateInvalidUrl() {
        DatasourceSettings.DatasourceEndpointValidator validator = new DatasourceSettings.DatasourceEndpointValidator();
        Exception e = expectThrows(IllegalArgumentException.class, () -> validator.validate("InvalidUrl"));
        assertEquals("Invalid URL format is provided", e.getMessage());

    }

    public void testValidateValidUrl() {
        DatasourceSettings.DatasourceEndpointValidator validator = new DatasourceSettings.DatasourceEndpointValidator();
        validator.validate("https://test.com");
    }

}

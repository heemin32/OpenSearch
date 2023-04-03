/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.common;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Settings for GeoIP datasource operations
 */
public class DatasourceSettings {

    /**
     * Default endpoint to be used in GeoIP datasource creation API
     */
    public static final Setting<String> DATASOURCE_ENDPOINT = Setting.simpleString(
        "ingest.geoip.datasource.endpoint",
        "https://geoip.maps.opensearch.org/v1/geolite-2/manifest.json",
        new DatasourceEndpointValidator(),
        Setting.Property.NodeScope
    );

    /**
     * Default update interval to be used in GeoIP datasource creation API
     */
    public static final Setting<TimeValue> DATASOURCE_UPDATE_INTERVAL = Setting.timeSetting(
        "ingest.geoip.datasource.update_interval",
        TimeValue.timeValueDays(3),
        TimeValue.timeValueDays(1),
        Setting.Property.NodeScope
    );

    public static class DatasourceEndpointValidator implements Setting.Validator<String> {
        @Override
        public void validate(final String value) {
            try {
                new URL(value).toURI();
            } catch (MalformedURLException | URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URL format is provided");
            }
        }
    }
}

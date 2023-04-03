/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.common;

import junit.framework.TestCase;
import org.opensearch.common.unit.TimeValue;

import java.time.Instant;

import static org.opensearch.ingest.geoip.IngestGeoIpModulePlugin.GEOIP_DATASOURCE_INDEX_NAME;

public class DatasourceMetadataTests extends TestCase {
    public void testGetIndexName() {
        DatasourceMetadata metadata = new DatasourceMetadata("test");
        metadata.setDatabase(new DatasourceMetadata.Database());
        metadata.getDatabase().setUpdatedAt(10l);
        assertEquals(String.format("%s.test.10", GEOIP_DATASOURCE_INDEX_NAME), metadata.getIndexName());
    }

    public void testGetIndexNameFor() {
        DatasourceMetadata metadata = new DatasourceMetadata("test");
        assertEquals(String.format("%s.test.10", GEOIP_DATASOURCE_INDEX_NAME), metadata.getIndexNameFor(10l));
    }

    public void testGetDelay() {
        DatasourceMetadata metadata = new DatasourceMetadata("test");
        metadata.setExpireAt(null);
        metadata.setNextUpdate(null);
        long now = Instant.now().getEpochSecond();
        assertEquals(TimeValue.timeValueSeconds(Long.MAX_VALUE - now), metadata.getDelayFrom(now));

        metadata.setExpireAt(now + 10l);
        metadata.setNextUpdate(null);
        assertEquals(TimeValue.timeValueSeconds(10l), metadata.getDelayFrom(now));

        metadata.setExpireAt(null);
        metadata.setNextUpdate(now + 11l);
        assertEquals(TimeValue.timeValueSeconds(11l), metadata.getDelayFrom(now));

        metadata.setExpireAt(now + 10l);
        metadata.setNextUpdate(now + 11l);
        assertEquals(TimeValue.timeValueSeconds(10l), metadata.getDelayFrom(now));

        metadata.setExpireAt(now + 11l);
        metadata.setNextUpdate(now + 10l);
        assertEquals(TimeValue.timeValueSeconds(10l), metadata.getDelayFrom(now));

        metadata.setExpireAt(11l);
        metadata.setNextUpdate(10l);
        assertEquals(TimeValue.timeValueSeconds(0l), metadata.getDelayFrom(now));
    }

}

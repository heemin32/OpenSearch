/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource.put;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

public class PutDatasourceAction extends ActionType<AcknowledgedResponse> {

    public static final PutDatasourceAction INSTANCE = new PutDatasourceAction();
    public static final String NAME = "cluster:admin/geoip/datasource/put";

    private PutDatasourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}

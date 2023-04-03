/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.geoip.datasource;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.ingest.geoip.datasource.common.DatasourceManifest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * GeoIP datasource creation request
 */
public class PutDatasourceRequest extends AcknowledgedRequest<PutDatasourceRequest> {
    private static final Logger LOGGER = LogManager.getLogger(PutDatasourceRequest.class);
    private static final ParseField ENDPOINT_FIELD = new ParseField("endpoint");
    private static final ParseField UPDATE_INTERVAL_FIELD = new ParseField("update_interval");
    private String id;
    private String endpoint;
    private TimeValue updateInterval;

    public static final ObjectParser<PutDatasourceRequest, Void> PARSER;
    static {
        PARSER = new ObjectParser<>("put_datasource");
        PARSER.declareString((request, val) -> request.setEndpoint(val), ENDPOINT_FIELD);
        PARSER.declareLong((request, val) -> request.setUpdateInterval(TimeValue.timeValueDays(val)), UPDATE_INTERVAL_FIELD);
    }

    public PutDatasourceRequest(final String id) {
        this.id = id;
    }

    public PutDatasourceRequest(final StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.endpoint = in.readString();
        this.updateInterval = in.readTimeValue();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeString(endpoint);
        out.writeTimeValue(updateInterval);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = new ActionRequestValidationException();
        validateEndpoint(errors);
        validateUpdateInterval(errors);
        return errors.validationErrors().isEmpty() ? null : errors;
    }

    private void validateEndpoint(final ActionRequestValidationException errors) {
        try {
            URL url = new URL(endpoint);
            url.toURI(); // Validate URL complies with RFC-2396
            AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    validateManifestFile(url, errors);
                    return null;
                }
            );
        } catch (MalformedURLException | URISyntaxException e) {
            LOGGER.info("Invalid URL format is provided", e);
            errors.addValidationError("Invalid URL format is provided");
        }
    }

    @SuppressForbidden(reason = "Need to connect to http endpoint for geoip database file")
    private void validateManifestFile(final URL url, final ActionRequestValidationException errors) {
        try {
            DatasourceManifest manifest = DatasourceManifest.Builder.build(url);
            new URL(manifest.getUrl()).toURI(); // Validate URL complies with RFC-2396
            if (manifest.getValidFor() <= updateInterval.days()) {
                errors.addValidationError(
                    String.format("updateInterval %d is should be smaller than %d", updateInterval.days(), manifest.getValidFor())
                );
            }
        } catch (StreamReadException | DatabindException e) {
            LOGGER.info("Failed to parse the manifest file", e);
            errors.addValidationError("Failed to parse the manifest file");
        } catch (MalformedURLException | URISyntaxException e) {
            LOGGER.info("Invalid URL format is provided for url field in the manifest file", e);
            errors.addValidationError("Invalid URL format is provided for url field in the manifest file");
        } catch (IOException e) {
            LOGGER.info("Failed to read {}", url, e);
            errors.addValidationError(String.format("Failed to read %s", url));
        }
    }

    private void validateUpdateInterval(final ActionRequestValidationException errors) {
        if (updateInterval.compareTo(TimeValue.timeValueDays(1)) > 0) {
            errors.addValidationError("Update interval should be equal to or larger than 1 day");
        }
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }

    public TimeValue getUpdateInterval() {
        return updateInterval;
    }

    public void setUpdateInterval(final TimeValue updateInterval) {
        this.updateInterval = updateInterval;
    }
}

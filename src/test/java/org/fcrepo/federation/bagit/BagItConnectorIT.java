
package org.fcrepo.federation.bagit;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

public class BagItConnectorIT extends AbstractResourceIT {

    @Test
    public void tryOneObject() throws ClientProtocolException, IOException {
        logger.debug("Found objects: " +
                EntityUtils.toString(client.execute(
                        new HttpGet(serverAddress + "objects/")).getEntity()));
        final String objName = "BagItFed1";
        final HttpResponse response =
                client.execute(new HttpGet(serverAddress + "objects/" + objName));
        assertEquals("Couldn't find federated object!", 200, response
                .getStatusLine().getStatusCode());
    }
}

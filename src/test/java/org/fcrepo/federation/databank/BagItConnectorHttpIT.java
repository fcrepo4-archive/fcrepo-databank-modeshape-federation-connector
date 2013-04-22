
package org.fcrepo.federation.databank;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.inject.Inject;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.fcrepo.FedoraObject;
import org.fcrepo.services.PathService;
import org.junit.Test;
import org.modeshape.jcr.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BagItConnectorHttpIT extends AbstractResourceIT {
	private static Logger logger = LoggerFactory.getLogger(BagItConnectorHttpIT.class);
	
    @Test
	public void tryOneObject() throws ClientProtocolException, IOException {
        logger.debug("Found objects: " +
                EntityUtils.toString(client.execute(
                        new HttpGet(serverAddress + "objects/")).getEntity()));
        final String objName = "BagItFed1";
        final HttpResponse response =
                client.execute(new HttpGet(serverAddress + "objects/" + objName));
        assertEquals(response.getStatusLine().getReasonPhrase(), 200, response
                .getStatusLine().getStatusCode());
    }
}

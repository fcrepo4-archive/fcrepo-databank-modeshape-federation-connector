
package org.fcrepo.federation.bagit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.inject.Inject;
import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.fcrepo.services.ObjectService;
import org.junit.Ignore;
import org.junit.Test;
import org.modeshape.jcr.api.Session;
import org.modeshape.jcr.api.federation.FederationManager;
import org.modeshape.jcr.cache.NodeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BagItConnectorIT extends AbstractResourceIT {
	private static Logger logger = LoggerFactory.getLogger(BagItConnectorIT.class);
	
	@Test
	@Ignore
	public void tryProgrammaticAccess() throws RepositoryException {
		Session session = (Session)repo.login();
		NodeKey key = new NodeKey("/objects/testDS");
		Node node = session.getNode("/objects/testDS");
		logger.info("Got node at " + node.getPath());
		NodeIterator nodes = node.getNodes();
		assertTrue("/objects/testDS had no child nodes!", nodes.hasNext());
		assertEquals("jcr:content", nodes.nextNode().getName());
	}

    @Test
	public void tryOneObject() throws ClientProtocolException, IOException {
        logger.debug("Found objects: " +
                EntityUtils.toString(client.execute(
                        new HttpGet(serverAddress + "objects/")).getEntity()));
        final String objName = "testDS";
        final HttpResponse response =
                client.execute(new HttpGet(serverAddress + "objects/" + objName));
        assertEquals(response.getStatusLine().getReasonPhrase(), 200, response
                .getStatusLine().getStatusCode());
    }
}

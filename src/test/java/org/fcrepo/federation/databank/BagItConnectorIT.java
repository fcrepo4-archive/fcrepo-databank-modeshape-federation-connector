
package org.fcrepo.federation.bagit;

import static org.fcrepo.jaxb.responses.access.ObjectProfile.ObjectStates.A;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.fcrepo.FedoraObject;
import org.fcrepo.jaxb.responses.access.ObjectProfile;
import org.fcrepo.services.PathService;
import org.junit.Test;
import org.modeshape.jcr.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BagItConnectorIT extends AbstractResourceIT {
	private static Logger logger = LoggerFactory.getLogger(BagItConnectorIT.class);
	
	@Test
	public void tryProgrammaticAccess() throws RepositoryException {
		Session session = (Session)repo.login();
		Node node = session.getNode("/objects/BagItFed1");
		logger.info("Got node at " + node.getPath());
		PropertyIterator properties = node.getProperties("bagit:*");
		assertTrue(properties.hasNext());
		// Bag-Count: 1 of 1
		Property property = node.getProperty("bagit:Bag.Count");
		assertNotNull(property);
		assertEquals("1 of 1", property.getString());
		NodeIterator nodes = node.getNodes();
		assertTrue("/objects/testDS had no child nodes!", nodes.hasNext());
		Node child = nodes.nextNode();
		nodes = child.getNodes();
		assertEquals("jcr:content", nodes.nextNode().getName());
		FedoraObject obj = new FedoraObject(session, PathService.getObjectJcrNodePath("BagItFed1"));
	}

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


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
import org.junit.runner.RunWith;
import org.modeshape.jcr.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/spring-test/master.xml")
public class BagItConnectorIT {
	private static Logger logger = LoggerFactory.getLogger(BagItConnectorIT.class);
	
    @Inject
    Repository repo;

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

}

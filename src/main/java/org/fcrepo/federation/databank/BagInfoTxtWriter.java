package org.fcrepo.federation.bagit;

import java.io.Closeable;
import java.io.OutputStream;

import gov.loc.repository.bagit.impl.BagInfoTxtWriterImpl;
/**
 * Just a proxy to implement Closeable
 * @author ba2213
 *
 */
public class BagInfoTxtWriter extends BagInfoTxtWriterImpl implements Closeable {

	public BagInfoTxtWriter(OutputStream out, String encoding) {
		super(out, encoding);
	}

	public BagInfoTxtWriter(OutputStream out, String encoding, int lineLength, int indent) {
		super(out, encoding, lineLength, indent);
	}
	
}

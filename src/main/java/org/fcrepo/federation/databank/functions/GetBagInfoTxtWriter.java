package org.fcrepo.federation.databank.functions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.fcrepo.federation.databank.BagInfoTxtWriter;

import com.google.common.base.Function;

public class GetBagInfoTxtWriter implements Function<String, gov.loc.repository.bagit.BagInfoTxtWriter> {

	@Override
	public BagInfoTxtWriter apply(String input) {
		try {
			return new BagInfoTxtWriter(new FileOutputStream(new File(input)), "UTF-8", 79, 0); // 79 char length, 0 indent
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Could not open BagInfo writer at " + input, e);
		}
	}

}

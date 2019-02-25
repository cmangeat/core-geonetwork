//=============================================================================
//===	Copyright (C) 2001-2007 Food and Agriculture Organization of the
//===	United Nations (FAO-UN), United Nations World Food Programme (WFP)
//===	and United Nations Environment Programme (UNEP)
//===
//===	This program is free software; you can redistribute it and/or modify
//===	it under the terms of the GNU General Public License as published by
//===	the Free Software Foundation; either version 2 of the License, or (at
//===	your option) any later version.
//===
//===	This program is distributed in the hope that it will be useful, but
//===	WITHOUT ANY WARRANTY; without even the implied warranty of
//===	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//===	General Public License for more details.
//===
//===	You should have received a copy of the GNU General Public License
//===	along with this program; if not, write to the Free Software
//===	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
//===
//===	Contact: Jeroen Ticheler - FAO - Viale delle Terme di Caracalla 2,
//===	Rome - Italy. email: geonetwork@osgeo.org
//==============================================================================

package org.fao.geonet.kernel.mef;

import static org.fao.geonet.kernel.mef.MEFConstants.FILE_INFO;
import static org.fao.geonet.kernel.mef.MEFConstants.FILE_METADATA;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import org.fao.geonet.Constants;
import org.fao.geonet.ZipUtil;
import org.fao.geonet.constants.Geonet;
import org.fao.geonet.domain.AbstractMetadata;
import org.fao.geonet.domain.MetadataType;
import org.fao.geonet.domain.Pair;
import org.fao.geonet.domain.ReservedOperation;
import org.fao.geonet.kernel.datamanager.IMetadataUtils;
import org.fao.geonet.kernel.mef.MEFLib.Format;
import org.fao.geonet.kernel.mef.MEFLib.Version;
import org.fao.geonet.lib.Lib;
import org.fao.geonet.utils.IO;
import org.fao.geonet.utils.Log;

import jeeves.server.context.ServiceContext;

/**
 * Export MEF file
 */
class MEFExporter {
	/**
	 * Create a metadata folder according to MEF {@link Version} 1 specification and
	 * return file path.
	 * <p>
	 * Template or subtemplate could not be exported in MEF format. Use XML export
	 * instead.
	 *
	 * @param uuid
	 *            UUID of the metadata record to export.
	 * @param format
	 *            {@link org.fao.geonet.kernel.mef.MEFLib.Format}
	 * @return the path of the generated MEF file.
	 */
	public static Path doExport(ServiceContext context, String uuid, Format format, boolean skipUUID,
			boolean resolveXlink, boolean removeXlinkAttribute, boolean addSchemaLocation,
			boolean approved) throws Exception {

        //Search by ID, not by UUID
        Integer id = 
        		context.getBean(IMetadataUtils.class).findOneByUuid(uuid).getId();

        //Here we just care if we need the approved version explicitly.
        //IMetadataUtils already filtered draft for non editors.
        if(approved) {
        	id = Integer.valueOf(context.getBean(IMetadataUtils.class).getMetadataId(uuid));
        }
        
		return doExport(context, id, format, skipUUID, resolveXlink, removeXlinkAttribute, addSchemaLocation);
	}

	/**
	 * Create a metadata folder according to MEF {@link Version} 1 specification and
	 * return file path.
	 * <p>
	 * Template or subtemplate could not be exported in MEF format. Use XML export
	 * instead.
	 *
	 * @param id
	 *            unique ID of the metadata record to export.
	 * @param format
	 *            {@link org.fao.geonet.kernel.mef.MEFLib.Format}
	 * @return the path of the generated MEF file.
	 */
	public static Path doExport(ServiceContext context, Integer id, Format format, boolean skipUUID,
			boolean resolveXlink, boolean removeXlinkAttribute, boolean addSchemaLocation) throws Exception {
		Pair<AbstractMetadata, String> recordAndMetadata = MEFLib.retrieveMetadata(context, id, resolveXlink,
				removeXlinkAttribute, addSchemaLocation);
		return export(context, id, format, skipUUID, recordAndMetadata);
	}

	private static Path export(ServiceContext context, Integer id, Format format, boolean skipUUID,
			Pair<AbstractMetadata, String> recordAndMetadata)
			throws Exception, IOException, UnsupportedEncodingException, URISyntaxException {
		AbstractMetadata record = recordAndMetadata.one();
		String xmlDocumentAsString = recordAndMetadata.two();

		if (record.getDataInfo().getType() == MetadataType.SUB_TEMPLATE
				|| record.getDataInfo().getType() == MetadataType.TEMPLATE_OF_SUB_TEMPLATE) {
			throw new Exception("Cannot export sub template");
		}

		Path file = Files.createTempFile("mef-", ".mef");
		Path pubDir = Lib.resource.getDir(context, "public", record.getId());
		Path priDir = Lib.resource.getDir(context, "private", record.getId());

		try (FileSystem zipFs = ZipUtil.createZipFs(file)) {
			// --- save metadata
			byte[] binData = xmlDocumentAsString.getBytes(Constants.ENCODING);
			Files.write(zipFs.getPath(FILE_METADATA), binData);

			// --- save info file
			binData = MEFLib.buildInfoFile(context, record, format, pubDir, priDir, skipUUID)
					.getBytes(Constants.ENCODING);
			Files.write(zipFs.getPath(FILE_INFO), binData);

			if (format == Format.PARTIAL || format == Format.FULL) {
				if (Files.exists(pubDir) && !IO.isEmptyDir(pubDir)) {
					IO.copyDirectoryOrFile(pubDir, zipFs.getPath(pubDir.getFileName().toString()), false);
				}
			}

			if (format == Format.FULL) {
				try {
					Lib.resource.checkPrivilege(context, "" + record.getId(), ReservedOperation.download);
					if (Files.exists(priDir) && !IO.isEmptyDir(priDir)) {
						IO.copyDirectoryOrFile(priDir, zipFs.getPath(priDir.getFileName().toString()), false);
					}

				} catch (Exception e) {
					// Current user could not download private data
					Log.warning(Geonet.MEF,
							"Error encounteres while trying to import private resources of MEF file. MEF ID: " + id, e);

				}
			}
		}
		return file;
	}
}

// =============================================================================

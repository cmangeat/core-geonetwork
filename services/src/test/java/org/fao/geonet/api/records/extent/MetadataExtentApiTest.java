/*
 * Copyright (C) 2001-2016 Food and Agriculture Organization of the
 * United Nations (FAO-UN), United Nations World Food Programme (WFP)
 * and United Nations Environment Programme (UNEP)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 *
 * Contact: Jeroen Ticheler - FAO - Viale delle Terme di Caracalla 2,
 * Rome - Italy. email: geonetwork@osgeo.org
 */

package org.fao.geonet.api.records.extent;

import jeeves.server.context.ServiceContext;
import org.fao.geonet.domain.ISODate;
import org.fao.geonet.domain.Metadata;
import org.fao.geonet.domain.MetadataType;
import org.fao.geonet.kernel.DataManager;
import org.fao.geonet.kernel.SchemaManager;
import org.fao.geonet.kernel.UpdateDatestamp;
import org.fao.geonet.repository.SourceRepository;
import org.fao.geonet.services.AbstractServiceIntegrationTest;
import org.fao.geonet.utils.Xml;
import org.jdom.Element;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.DigestUtils;
import org.springframework.web.context.WebApplicationContext;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.UUID;

import static org.fao.geonet.schema.iso19139.ISO19139Namespaces.GCO;
import static org.fao.geonet.schema.iso19139.ISO19139Namespaces.GMD;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ContextConfiguration(inheritLocations = true, locations = "classpath:extents-test-context.xml")
public class MetadataExtentApiTest extends AbstractServiceIntegrationTest {

    @Autowired
    private DataManager dataManager;
    @Autowired
    private SourceRepository sourceRepository;
    @Autowired
    private SchemaManager schemaManager;
    @Autowired
    private WebApplicationContext wac;

    private ServiceContext context;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp() throws Exception {
        context = createServiceContext();
    }

    @Test
    public void nominal() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        MockHttpSession mockHttpSession = loginAsAdmin();
        String uuid = createTestData();

        byte[] reponseBuffer = mockMvc.perform(get(String.format("/srv/api/records/%s/extents.png", uuid))
            .session(mockHttpSession)
            .accept(MediaType.IMAGE_PNG_VALUE))
            .andExpect(status().is2xxSuccessful())
            .andExpect(content().contentType(API_PNG_EXPECTED_ENCODING))
            .andReturn().getResponse().getContentAsByteArray();

        //BufferedImage imag=ImageIO.read(new ByteArrayInputStream(reponseBuffer));
        //ImageIO.write(imag, "png", new File("/tmp", String.format("%s.png", name.getMethodName())));
        assertEquals("b02baec6d92832ecd5653db78093a427", DigestUtils.md5DigestAsHex(reponseBuffer));
    }

    @Test
    public void lastModifiedNotModified() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        MockHttpSession mockHttpSession = loginAsAdmin();
        String uuid = createTestData();

        mockMvc.perform(get(String.format("/srv/api/records/%s/extents.png", uuid))
            .header("If-Modified-Since", "Wed, 21 Oct 2015 07:29:00 UTC")
            .session(mockHttpSession)
            .accept(MediaType.IMAGE_PNG_VALUE))
            .andExpect(status().isNotModified());
    }

    @Test
    public void lastModifiedModified() throws Exception {
        MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        MockHttpSession mockHttpSession = loginAsAdmin();
        String uuid = createTestData();

        byte[] reponseBuffer = mockMvc.perform(get(String.format("/srv/api/records/%s/extents.png", uuid))
            .header("If-Modified-Since", "Wed, 21 Oct 2015 07:27:00 UTC")
            .session(mockHttpSession)
            .accept(MediaType.IMAGE_PNG_VALUE))
            .andExpect(status().is2xxSuccessful())
            .andExpect(content().contentType(API_PNG_EXPECTED_ENCODING))
            .andReturn().getResponse().getContentAsByteArray();

        assertEquals("b02baec6d92832ecd5653db78093a427", DigestUtils.md5DigestAsHex(reponseBuffer));
    }

    private String createTestData() throws Exception {
        loginAsAdmin(context);

        Element sampleMetadataXml = getSampleMetadataXml();
        String uuid = UUID.randomUUID().toString();
        Xml.selectElement(sampleMetadataXml, "gmd:fileIdentifier/gco:CharacterString", Arrays.asList(GMD, GCO)).setText(uuid);

        GregorianCalendar calendar = new GregorianCalendar();
        calendar.set(2015, Calendar.OCTOBER, 21, 07, 28, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));

        String source = sourceRepository.findAll().get(0).getUuid();
        String schema = schemaManager.autodetectSchema(sampleMetadataXml);
        Metadata metadata = (Metadata) new Metadata()
            .setDataAndFixCR(sampleMetadataXml)
            .setUuid(uuid);
        metadata.getDataInfo()
            .setRoot(sampleMetadataXml.getQualifiedName())
            .setSchemaId(schema)
            .setType(MetadataType.METADATA)
            .setPopularity(1000)
            .setChangeDate(new ISODate(calendar.getTimeInMillis()));
        metadata.getSourceInfo()
            .setOwner(1)
            .setSourceId(source);
        metadata.getHarvestInfo()
            .setHarvested(false);

        dataManager.insertMetadata(context, metadata, sampleMetadataXml, false, true, false, UpdateDatestamp.NO,
            false, false).getId();

        return uuid;
    }
}

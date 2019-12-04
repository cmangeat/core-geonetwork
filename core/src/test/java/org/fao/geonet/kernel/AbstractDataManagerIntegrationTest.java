package org.fao.geonet.kernel;

import com.google.common.base.Optional;
import jeeves.server.context.ServiceContext;
import org.fao.geonet.AbstractCoreIntegrationTest;
import org.fao.geonet.constants.Params;
import org.fao.geonet.domain.AbstractMetadata;
import org.fao.geonet.domain.Metadata;
import org.fao.geonet.domain.MetadataType;
import org.fao.geonet.domain.ReservedGroup;
import org.fao.geonet.kernel.datamanager.IMetadataManager;
import org.fao.geonet.repository.MetadataRepository;
import org.fao.geonet.repository.Updater;
import org.fao.geonet.utils.Xml;
import org.jdom.Element;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AbstractDataManagerIntegrationTest extends AbstractCoreIntegrationTest {

    @Autowired
    protected DataManager dataManager;

    @Autowired
    protected IMetadataManager metadataManager;

    @Autowired
    protected MetadataRepository metadataRepository;

    protected void doSetHarvesterDataTest(int metadataId) throws Exception {
        AbstractMetadata metadata = metadataRepository.findOne(metadataId);

        assertNull(metadata.getHarvestInfo().getUuid());
        assertNull(metadata.getHarvestInfo().getUri());
        assertFalse(metadata.getHarvestInfo().isHarvested());

        final String harvesterUuid = "harvesterUuid";
        dataManager.setHarvestedExt(metadataId, harvesterUuid);
        metadata = metadataRepository.findOne(metadataId);
        assertEquals(harvesterUuid, metadata.getHarvestInfo().getUuid());
        assertTrue(metadata.getHarvestInfo().isHarvested());
        assertNull(metadata.getHarvestInfo().getUri());


        final String newSource = "newSource";
        // check that another update doesn't break the last setting
        // there used to a bug where this was the case because entity manager wasn't being flushed
        metadataRepository.update(metadataId, new Updater<Metadata>() {
            @Override
            public void apply(@Nonnull Metadata entity) {
                entity.getSourceInfo().setSourceId(newSource);
            }
        });

        assertEquals(newSource, metadata.getSourceInfo().getSourceId());
        assertEquals(harvesterUuid, metadata.getHarvestInfo().getUuid());
        assertTrue(metadata.getHarvestInfo().isHarvested());
        assertNull(metadata.getHarvestInfo().getUri());

        final String harvesterUuid2 = "harvesterUuid2";
        final String harvesterUri = "harvesterUri";
        dataManager.setHarvestedExt(metadataId, harvesterUuid2, Optional.of(harvesterUri));
        metadata = metadataRepository.findOne(metadataId);
        assertEquals(harvesterUuid2, metadata.getHarvestInfo().getUuid());
        assertTrue(metadata.getHarvestInfo().isHarvested());
        assertEquals(harvesterUri, metadata.getHarvestInfo().getUri());

        dataManager.setHarvestedExt(metadataId, null);
        metadata = metadataRepository.findOne(metadataId);
        assertNull(metadata.getHarvestInfo().getUuid());
        assertNull(metadata.getHarvestInfo().getUri());
        assertFalse(metadata.getHarvestInfo().isHarvested());
    }

    protected int importMetadata(ServiceContext serviceContext) throws Exception {
        final Element sampleMetadataXml = getSampleMetadataXml();
        final ByteArrayInputStream stream = new ByteArrayInputStream(Xml.getString(sampleMetadataXml).getBytes("UTF-8"));
        return importMetadataXML(serviceContext, "uuid", stream, MetadataType.METADATA,
                ReservedGroup.all.getId(), Params.GENERATE_UUID);
    }

    protected ServiceContext createContextAndLogAsAdmin() throws Exception {
        ServiceContext context = createServiceContext();
        loginAsAdmin(context);
        return context;
    }

}

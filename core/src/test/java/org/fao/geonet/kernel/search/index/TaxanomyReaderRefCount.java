package org.fao.geonet.kernel.search.index;

import jeeves.server.context.ServiceContext;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.fao.geonet.AbstractCoreIntegrationTest;
import org.fao.geonet.constants.Geonet;
import org.fao.geonet.domain.AbstractMetadata;
import org.fao.geonet.domain.Metadata;
import org.fao.geonet.domain.MetadataType;
import org.fao.geonet.kernel.AbstractIntegrationTestWithMockedSingletons;
import org.fao.geonet.kernel.SchemaManager;
import org.fao.geonet.kernel.SpringLocalServiceInvoker;
import org.fao.geonet.kernel.datamanager.IMetadataManager;
import org.fao.geonet.kernel.search.IndexAndTaxonomy;
import org.fao.geonet.kernel.search.SearchManager;
import org.fao.geonet.repository.SourceRepository;
import org.fao.geonet.utils.Xml;
import org.jdom.Element;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URL;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.fao.geonet.domain.MetadataType.SUB_TEMPLATE;
import static org.fao.geonet.kernel.UpdateDatestamp.NO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TaxanomyReaderRefCount extends AbstractIntegrationTestWithMockedSingletons {

    private static final int TEST_OWNER = 42;

    @Autowired
    private IMetadataManager metadataManager;

    @Autowired
    private SchemaManager schemaManager;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    private SearchManager searchManager;

    @Autowired
    private LuceneIndexLanguageTracker luceneIndexLanguageTracker;

    private ServiceContext context;

    @Before
    public void setUp() throws Exception {
        this.context = createServiceContext();
    }

    @Test
    public void testRefCount() throws Exception {
        TaxonomyIndexTracker taxonomyIndexTracker = (TaxonomyIndexTracker) Whitebox.getInternalState(luceneIndexLanguageTracker, "taxonomyIndexTracker");
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term(Geonet.IndexFieldNames.ANY, "babar")), BooleanClause.Occur.MUST);
        insertContact();
        insertContact();
        IndexAndTaxonomy indexReader1 = searchManager.getIndexReader(null, -1);
        IndexAndTaxonomy indexReader2 = searchManager.getIndexReader(null, -1);
        insertContact();
        IndexAndTaxonomy indexReader3 = searchManager.getIndexReader(null, -1);
        insertContact();
        IndexAndTaxonomy indexReader4 = searchManager.getIndexReader(null, -1);
        assertEquals(2, new IndexSearcher(indexReader1.indexReader).search(query, 5).totalHits);
        assertEquals(2, new IndexSearcher(indexReader2.indexReader).search(query, 5).totalHits);
        assertEquals(3, new IndexSearcher(indexReader3.indexReader).search(query, 5).totalHits);
        assertEquals(4, new IndexSearcher(indexReader4.indexReader).search(query, 5).totalHits);

        List<DirectoryTaxonomyReader> expiredReadersSoFar = (List<DirectoryTaxonomyReader>) Whitebox.getInternalState(taxonomyIndexTracker, "expiredReaders");
        assertEquals(1, expiredReadersSoFar.size());   // ???
        assertEquals(1, expiredReadersSoFar.get(0).getRefCount());  // ???
        assertFalse((Boolean) Whitebox.getInternalState(expiredReadersSoFar.get(0), "closed"));
        DirectoryTaxonomyReader currentReader = (DirectoryTaxonomyReader) Whitebox.getInternalState(taxonomyIndexTracker, "taxonomyReader");
        assertEquals(5, currentReader.getRefCount());  // ???  --> What do I have to do to make taxonomy reader expire ?
        assertFalse((Boolean) Whitebox.getInternalState(currentReader, "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader1.indexReader, "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader2.indexReader, "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader3.indexReader, "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader4.indexReader, "closed"));

        indexReader1.close();
        indexReader2.close();
        indexReader3.close();
        indexReader4.close();

        assertFalse((Boolean) Whitebox.getInternalState(indexReader1.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader2.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader3.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader4.indexReader, "closed"));  // !!!
        expiredReadersSoFar = (List<DirectoryTaxonomyReader>) Whitebox.getInternalState(taxonomyIndexTracker, "expiredReaders");
        assertEquals(1, expiredReadersSoFar.size());   // ???
        assertEquals(1, expiredReadersSoFar.get(0).getRefCount());  // ???
        assertFalse((Boolean) Whitebox.getInternalState(expiredReadersSoFar.get(0), "closed"));
        currentReader = (DirectoryTaxonomyReader) Whitebox.getInternalState(taxonomyIndexTracker, "taxonomyReader");
        assertEquals(5, currentReader.getRefCount());  // ???
        assertFalse((Boolean) Whitebox.getInternalState(currentReader, "closed"));

        currentReader.decRef();
        currentReader.decRef();
        currentReader.decRef();
        currentReader.decRef();
        assertFalse((Boolean) Whitebox.getInternalState(expiredReadersSoFar.get(0), "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader1.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader2.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader3.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader4.indexReader, "closed"));  // !!!

        currentReader.decRef();
        assertFalse((Boolean) Whitebox.getInternalState(expiredReadersSoFar.get(0), "closed"));
        assertFalse((Boolean) Whitebox.getInternalState(indexReader1.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader2.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader3.indexReader, "closed"));  // !!!
        assertFalse((Boolean) Whitebox.getInternalState(indexReader4.indexReader, "closed"));  // !!!

        indexReader1.indexReader.decRef();  // or indexReader1.close()
        indexReader2.indexReader.decRef();  // or indexReader2.close()
        indexReader3.indexReader.close();
        indexReader4.indexReader.close();

        // really don't know whether they have to be kept open or not
        assertTrue((Boolean) Whitebox.getInternalState(indexReader1.indexReader, "closed"));  // !!!
        assertTrue((Boolean) Whitebox.getInternalState(indexReader2.indexReader, "closed"));  // !!!
        assertTrue((Boolean) Whitebox.getInternalState(indexReader3.indexReader, "closed"));  // !!!
        assertTrue((Boolean) Whitebox.getInternalState(indexReader4.indexReader, "closed"));  // !!!


    }


    private AbstractMetadata insertTemplateResourceInDb(Element element, MetadataType type) throws Exception {
        loginAsAdmin(context);

        Metadata metadata = new Metadata();
        metadata
                .setDataAndFixCR(element)
                .setUuid(UUID.randomUUID().toString());
        metadata.getDataInfo()
                .setRoot(element.getQualifiedName())
                .setSchemaId(schemaManager.autodetectSchema(element))
                .setType(type)
                .setPopularity(1000);
        metadata.getSourceInfo()
                .setOwner(TEST_OWNER)
                .setSourceId(sourceRepository.findAll().get(0).getUuid());
        metadata.getHarvestInfo()
                .setHarvested(false);

        AbstractMetadata dbInsertedMetadata = metadataManager.insertMetadata(
                context,
                metadata,
                element,
                false,
                true,
                false,
                NO,
                false,
                false);

        return dbInsertedMetadata;
    }

    private AbstractMetadata insertContact() throws Exception {
        URL contactResource = AbstractCoreIntegrationTest.class.getResource("kernel/babarContact.xml");
        Element contactElement = Xml.loadStream(contactResource.openStream());
        return insertContact(contactElement);
    }

    private AbstractMetadata insertContact( Element contactElement) throws Exception {
        AbstractMetadata contactMetadata = insertTemplateResourceInDb(contactElement, SUB_TEMPLATE);
        SpringLocalServiceInvoker mockInvoker = resetAndGetMockInvoker();
        when(mockInvoker.invoke(any(String.class))).thenReturn(contactElement);
        return contactMetadata;
    }

}

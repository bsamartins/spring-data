package org.bsamartins.spring.data.mongo.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import org.bsamartins.spring.data.mongo.AsyncInputStreamHelper;
import org.bsamartins.spring.data.mongo.MongoTestConfig;
import org.bsamartins.spring.data.mongo.ReactiveMongoTestConfig;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {MongoTestConfig.class, ReactiveGridFsTemplateIntegrationTest.TestConfig.class})
public class ReactiveGridFsTemplateIntegrationTest {

    private AsyncInputStream asyncInputStream;
    private long dataLength;

    @Configuration
    public static class TestConfig extends ReactiveMongoTestConfig {

        @Bean
        public ReactiveGridFsTemplate reactiveGridFsTemplate() throws Exception {
            return new ReactiveGridFsTemplate(reactiveMongoDbFactory(), mappingMongoConverter());
        }

    }

    @Autowired
    private ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory;

    @Autowired
    private ReactiveGridFsTemplate operations;

    @BeforeEach
    public void setup() {
        byte[] data = "Hello World".getBytes();
        this.asyncInputStream = toAsyncInputStream(data);
        this.dataLength = data.length;
    }

    private AsyncInputStream toAsyncInputStream(byte[] data) {
        return AsyncInputStreamHelper.from(new ByteArrayInputStream(data));
    }

    @Test // DATAMONGO-6
    public void storesAndFindsSimpleDocument() throws IOException {

        ObjectId reference = operations.store(asyncInputStream, "foo.xml").block();

        List<GridFSFile> files = operations.find(query(where("_id").is(reference)))
                .collectList()
                .block();
        assertThat(files.size(), is(1));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-6
    public void writesMetadataCorrectly() {

        Document metadata = new Document("key", "value");

        ObjectId reference = operations.store(asyncInputStream, "foo.xml", metadata)
                .block();

        List<GridFSFile> files  = operations.find(query(whereMetaData("key").is("value")))
                .collectList()
                .block();

        assertThat(files.size(), is(1));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-6
    public void marshalsComplexMetadata() {

        Metadata metadata = new Metadata();
        metadata.version = "1.0";

        ObjectId reference = operations.store(asyncInputStream, "foo.xml", metadata)
                .block();

        List<GridFSFile> files = operations.find(query(whereFilename().is("foo.xml")))
                .collectList()
                .block();

        assertThat(files.size(), is(1));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-6
    public void findsFilesByResourcePattern() {

        ObjectId reference = operations.store(asyncInputStream, "foo.xml")
                .block();

        List<ReactiveGridFsResource> resources = operations.getResources("*.xml")
                .collectList()
                .block();

        assertThat(resources.size(), is(1));
        assertThat(((BsonObjectId) resources.get(0).getId()).getValue(), is(reference));
        assertThat(resources.get(0).getContentLength(), is(dataLength));
        // assertThat(resources[0].getContentType(), is(resource.()));
    }

    @Test // DATAMONGO-6
    public void findsFilesByResourceLocation() {

        ObjectId reference = operations.store(asyncInputStream, "foo.xml").block();

        List<ReactiveGridFsResource> resources = operations.getResources("foo.xml")
                .collectList()
                .block();
        assertThat(resources.size(), is(1));
        assertThat(((BsonObjectId) resources.get(0).getId()).getValue(), is(reference));
        assertThat(resources.get(0).getContentLength(), is(dataLength));
        // assertThat(resources.get(0).getContentType(), is(reference.getContentType()));
    }

    @Test // DATAMONGO-503
    public void storesContentType() {

        ObjectId reference = operations.store(asyncInputStream, "foo2.xml", "application/xml")
                .block();

        List<GridFSFile> result = operations.find(query(whereContentType().is("application/xml")))
                .collectList()
                .block();

        assertThat(result.size(), is(1));
        assertEquals(((BsonObjectId) result.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-534
    public void considersSortWhenQueryingFiles() {
        ObjectId second = operations.store(asyncInputStream, "foo.xml").block();
        ObjectId third = operations.store(asyncInputStream, "foobar.xml").block();
        ObjectId first = operations.store(asyncInputStream, "bar.xml").block();

        Query query = new Query().with(Sort.by(Sort.Direction.ASC, "filename"));

        List<GridFSFile> files = operations.find(query)
                .collectList()
                .block();

        files.forEach(System.out::println);

        assertThat(files, hasSize(3));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), first);
        assertEquals(((BsonObjectId) files.get(1).getId()).getValue(), second);
        assertEquals(((BsonObjectId) files.get(2).getId()).getValue(), third);
    }

    @Test // DATAMONGO-534, DATAMONGO-1762
    public void queryingWithEmptyQueryReturnsAllFiles() {

        ObjectId reference = operations.store(asyncInputStream, "foo.xml")
                .block();

        List<GridFSFile> files = operations.find(new Query())
                .collectList()
                .block();

        assertThat(files, hasSize(1));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-1762
    public void queryingWithNullQueryThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> operations.find(null));
    }

    @Test // DATAMONGO-813
    public void getResourceShouldReturnEmptyForNonExistingResource() {
        ReactiveGridFsResource resource = operations.getResource("doesnotexist").block();
        assertNull(resource);
    }

    @Test // DATAMONGO-809
    public void storesAndFindsSimpleDocumentWithMetadataDocument() {

        Document metadata = new Document("key", "value");
        ObjectId reference = operations.store(asyncInputStream, "foobar", metadata).block();

        List<GridFSFile> files = operations.find(query(whereMetaData("key").is("value")))
                .collectList()
                .block();

        assertThat(files, hasSize(1));
        assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-809
    public void storesAndFindsSimpleDocumentWithMetadataObject() {

        Metadata metadata = new Metadata();
        metadata.version = "1.0";
        ObjectId reference = operations.store(asyncInputStream, "foobar", metadata).block();

        List<GridFSFile> result = operations.find(query(whereMetaData("version").is("1.0")))
                .collectList()
                .block();

        assertThat(result, hasSize(1));
        assertEquals(((BsonObjectId) result.get(0).getId()).getValue(), reference);
    }

    @Test // DATAMONGO-1695
    public void readsContentTypeCorrectly() {
        Mono<ReactiveGridFsResource> result = operations.store(asyncInputStream, "someName", "contentType")
            .flatMap(id -> operations.getResource("someName"));

        StepVerifier.create(result)
                .assertNext(f -> assertEquals(f.getContentType(), "contentType"))
                .verifyComplete();
    }

    @AfterEach
    public void tearDown() {
        Mono.from(reactiveMongoDatabaseFactory.getMongoDatabase().drop())
                .subscribe();
    }

    class Metadata {
        String version;
    }
}

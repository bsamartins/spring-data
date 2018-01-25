package pt.bsamartins.spring.data.mongo.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import org.bson.BsonValue;
import org.springframework.util.Assert;

import java.util.Optional;


/**
 * Representation of a reactive GridFS resource
 *
 * @author Bernardo Martins
 */
public final class ReactiveGridFsResource {

    private GridFSFile file;
    private AsyncInputStream asyncInputStream;

    /**
     *
     * @param file must not be {@literal null}
     * @param asyncInputStream must not be {@literal null}
     */
    ReactiveGridFsResource(GridFSFile file, AsyncInputStream asyncInputStream) {
        Assert.notNull(file, "file must not be null");
        Assert.notNull(asyncInputStream, "asyncInputStream must not be null");

        this.file = file;
        this.asyncInputStream = asyncInputStream;
    }

    /**
     * Gets the resource id
     *
     * @return a BsonValue representing the resource id
     */
    public BsonValue getId() {
        return this.file.getId();
    }

    /**
     * Gets the filename
     *
     * @return the resource filename
     */
    public String getFilename() {
        return this.file.getFilename();
    }

    /**
     * Gets the content type
     *
     * @return the content type
     */
    public String getContentType() {
        return Optional.ofNullable(file.getMetadata())
                .map(doc -> doc.getString(GridsFsHeaderConstants.CONTENT_TYPE_FIELD))
                .orElse(null);
    }

    /**
     * Gets the resource content length
     *
     * @return the content length
     */
    public long getContentLength() {
        return file.getLength();
    }

    /**
     * Gets the stream to the file
     *
     * @return the file stream
     */
    public AsyncInputStream getAsyncInputStream() {
        return asyncInputStream;
    }
}

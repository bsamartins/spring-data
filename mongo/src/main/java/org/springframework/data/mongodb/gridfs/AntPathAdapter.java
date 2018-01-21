package org.springframework.data.mongodb.gridfs;

import org.springframework.lang.NonNull;

/**
 *
 * Exposes package class {@link AntPath}
 *
 */
public class AntPathAdapter extends AntPath {
    /**
     * Creates a new {@link AntPath} from the given path.
     *
     * @param path must not be {@literal null}.
     */
    public AntPathAdapter(@NonNull String path) {
        super(path);
    }
}

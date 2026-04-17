// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ──────────────────────────────────────────────────

// Write an object to the store.
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Format the header
    const char *type_str = type == OBJ_BLOB ? "blob" : (type == OBJ_TREE ? "tree" : "commit");
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1; // +1 for \0

    // 2. Combine header and data
    size_t total_len = header_len + len;
    uint8_t *buffer = malloc(total_len);
    if (!buffer) return -1;
    memcpy(buffer, header, header_len);
    memcpy(buffer + header_len, data, len);

    // 3. Compute hash
    compute_hash(buffer, total_len, id_out);

    // 4. Deduplication check
    if (object_exists(id_out)) {
        free(buffer);
        return 0; 
    }

    // 5. Prepare directory shard
    char path[512];
    object_path(id_out, path, sizeof(path));
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%.*s", (int)(strrchr(path, '/') - path), path);
    mkdir(dir_path, 0755); 

    // 6. Atomic write (temp file -> rename)
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/tmp.XXXXXX", dir_path);
    int fd = mkstemp(tmp_path);
    if (fd < 0) { 
        free(buffer); 
        return -1; 
    }
    
    write(fd, buffer, total_len);
    fsync(fd);
    close(fd);
    rename(tmp_path, path);
    
    free(buffer);
    return 0;
}

// Read an object from the store.
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Read entire file
    fseek(f, 0, SEEK_END);
    size_t total_len = ftell(f);
    fseek(f, 0, SEEK_SET);

    uint8_t *buffer = malloc(total_len);
    if (!buffer) {
        fclose(f);
        return -1;
    }
    fread(buffer, 1, total_len, f);
    fclose(f);

    // Integrity Check
    ObjectID computed_id;
    compute_hash(buffer, total_len, &computed_id);
    if (memcmp(id->hash, computed_id.hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    // Parse header
    uint8_t *null_byte = memchr(buffer, '\0', total_len);
    if (!null_byte) {
        free(buffer);
        return -1;
    }
    
    char type_str[32];
    if (sscanf((char *)buffer, "%31s %zu", type_str, len_out) != 2) {
        free(buffer);
        return -1;
    }

    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else {
        free(buffer);
        return -1;
    }

    // Extract payload
    *data_out = malloc(*len_out);
    if (!*data_out) {
        free(buffer);
        return -1;
    }
    memcpy(*data_out, null_byte + 1, *len_out);
    
    free(buffer);
    return 0;
}

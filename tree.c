#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// external from object.c
extern int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────

#define MODE_FILE 0100644
#define MODE_EXEC 0100755
#define MODE_DIR  0040000

// ─── PROVIDED ───────────────────────────────────

// Get file mode
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode)) return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse tree object
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;

    const uint8_t *ptr = data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // parse mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;

        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1;

        // parse name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;

        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        // parse hash
        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }

    return 0;
}

// sorting helper
static int compare_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name,
                  ((const TreeEntry *)b)->name);
}

// Serialize tree object
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 300;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // sort entries (important for deterministic output)
    Tree sorted = *tree;
    qsort(sorted.entries, sorted.count,
          sizeof(TreeEntry), compare_entries);

    size_t offset = 0;

    for (int i = 0; i < sorted.count; i++) {
        const TreeEntry *e = &sorted.entries[i];

        int written = sprintf((char *)buffer + offset,
                              "%o %s", e->mode, e->name);

        offset += written + 1; // include '\0'

        memcpy(buffer + offset, e->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── IMPORTANT FOR PHASE 2 ─────────────────────

// Keep this simple to avoid linker errors
int tree_from_index(ObjectID *id_out) {
    (void)id_out;
    return 0;
}

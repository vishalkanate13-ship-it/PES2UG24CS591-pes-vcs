// index.c — Staging area implementation

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTED ───────────────────────────────────────────────────────────

// Load the index from .pes/index.
int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0; // Empty/missing index is perfectly fine for a new repo

    char line[1024];
    while (fgets(line, sizeof(line), f) && index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *entry = &index->entries[index->count];
        char hex[HASH_HEX_SIZE + 1];
        unsigned long long mtime;
        
        if (sscanf(line, "%o %64s %llu %u %511[^\n]", 
                   &entry->mode, hex, &mtime, &entry->size, entry->path) == 5) {
            if (hex_to_hash(hex, &entry->hash) == 0) {
                entry->mtime_sec = mtime;
                index->count++;
            }
        }
    }
    fclose(f);
    return 0;
}

// Helper function for qsort to alphabetize the index
static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// Save the index to .pes/index atomically.
int index_save(const Index *index) {
    // HEAP FIX: Allocate 5.6MB on the heap instead of the stack!
    Index *sorted = malloc(sizeof(Index));
    if (!sorted) return -1;

    *sorted = *index;
    qsort(sorted->entries, sorted->count, sizeof(IndexEntry), compare_index_entries);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);
    
    FILE *f = fopen(tmp_path, "w");
    if (!f) {
        free(sorted);
        return -1;
    }

    for (int i = 0; i < sorted->count; i++) {
        const IndexEntry *entry = &sorted->entries[i];
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&entry->hash, hex);
        fprintf(f, "%o %s %llu %u %s\n", 
                entry->mode, hex, (unsigned long long)entry->mtime_sec, entry->size, entry->path);
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    free(sorted); // Give the memory back
    return rename(tmp_path, INDEX_FILE);
}

// Stage a file for the next commit.
int index_add(Index *index, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: could not open '%s'\n", path);
        return -1;
    }

    // Get file size
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    fseek(f, 0, SEEK_SET);

    // Read file contents
    void *data = malloc(size);
    if (size > 0 && data) {
        if (fread(data, 1, size, f) != size) {
            free(data);
            fclose(f);
            return -1;
        }
    }
    fclose(f);

    // Hash and store the blob
    extern int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
    ObjectID hash;
    if (object_write(OBJ_BLOB, data, size, &hash) != 0) {
        free(data);
        return -1;
    }
    free(data);

    // Get file metadata
    struct stat st;
    if (stat(path, &st) != 0) return -1;

    // Update existing entry or create a new one
    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        entry = &index->entries[index->count++];
    }

    uint32_t mode = 0100644; // Default to standard file
    if (S_ISDIR(st.st_mode)) mode = 0040000;
    else if (st.st_mode & S_IXUSR) mode = 0100755;

    entry->mode = mode;
    entry->hash = hash;
    entry->mtime_sec = st.st_mtime;
    entry->size = size;
    strncpy(entry->path, path, sizeof(entry->path) - 1);

    return index_save(index);
}

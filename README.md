# Buffer-Manager
Advance Database Organization - CS 525

Team Members :

Sharul Saxena
Neilabh Okhandiar
Jayavarshini Ilarajan
Obafemi Shyllon

# USAGE

run `make` to produce the `test_assign2` binary

Execution will simply run the tests defined in `test_assign2_1.c`

## EXPLAINING THE CODE

Most of the functionality exists in `buffer_mgr.c` and `storage_mgr.c`. dberror, test_assign1 and test_helpers are all
helper functionality; dberror defined our general ERROR CODE int `RC`, and defined PAGE_SIZE as 4096 [bytes].

This README will explain `buffer_mgr.c`; A description of the other files can found in the `assign1/` directory.

The Buffer Manager handles the page-accesses of a single pagefile. It maintains a pool of in-memory pages (known as frames),
and depends on the Storage Manager to actually get the page-data from disk. The pool acts as a cache of pages; If
multiple pages from the same file are required (a likely situation), and the required pages have been accessed before,
then hopefully it will be found in our buffer pool and the cost of hitting the disk can be avoided.

The Buffer Manager is threadsafe, in the sense that all necessary information is contained in the `BM_BUFFERPOOL` struct.
The same bufferpool CANNOT be shared between threads without running into race-conditions. If the a bufferpool is to be
shared, it is up to the client to provide proper locking mechanisms. But two calls to a buffer_mgr function with two
**different** bufferpool instances is guaranteed to be safe.

The Buffer Manager offers several page-replacement strategies, for when the pool is filled:
* FIFO - The first page to be pulled into memory will be the first page to be ejected
* LRU - The page with the oldest access will be the first page to be ejected
* CLOCK - An approximation of the LRU algorithm, with lower overhead
    * NOTE: The current implementation offers NO benefit over LRU. It is correct, but naively implemented.
* LFU - The page with the fewest accesses in its existence will be the first page to be ejected
    * NOTE: LFU is NOT recommended for usage. It suffers a number of problems, the biggest being that many accesses
    early on, followed by no accesses, will cause a useless page to stay almost permanently in the pool.
* LRU_K - The page with the K oldest access will be the first page to be ejected
    * ie if K = 2, and Page1 has an access history of (higher is newer) [1, 5], and Page2 has [3,4], then Page1 will be
    ejected.

### Public Functions

initBufferPool:
    Instantiates a new BM_BUFFERPOOL struct.

shutdownBufferPool:
    Should only be called if no pages are fixed.
    Flushes any dirty pages in the BufferPool, and frees all memory allocated to the pool

forceFlushPool:
    Should only be called if no pages are fixed.
    Flushes any dirty pages in the BufferPool

markDirty:
    Denotes the page has been written to, and needs to be (eventually) flushed to disk

pinPage:
    If the page exists in the buffer pool, just adds one to the fixed counter and returns the page
    else it ejects a page according to the replacement strategy (writes to disk if dirty),
    and pulls the new page from disk

    If all pages in the pool are fixed, it throws an error. (as no page can be ejected)

unpinPage:
    Drops the fixed counter by one for that page

forcePage:
    Writes the page to disk
    If the page is fixed, throws an error.

# Testing
testCreatingAndReadingDummyPages, testReadPage, testFIFO and testLRU were written by the professor, and thus do not
need explanation

testCLOCK, testLFU were added to test the relevant replacement strategies. Simply adds pages in a specific order, and
checks if pages were ejected in the correct order.

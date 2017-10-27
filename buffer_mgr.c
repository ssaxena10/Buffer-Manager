//
// Created by Neilabh Okhandiar on 9/28/17.
//
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>


// PageFrame is statically allocated
// BM_PageHandle is statically allocated
// The page data (frame.data) is dynamically allocated
typedef struct PageFrame {
    BM_PageHandle frame; // the in-memory page
    bool dirty;
    int fixcount;
    int counter;     // if it's FIFO, when this page was written into memory
                     // if it's LRU, when this page was last used
                     // if it's CLOCK, this is either 0 or 1
                     // if it's LFU, increment whenever a page pins it

    // for LFU-K, an array of timestamps for the last access
    int *accesses;
} PageFrame;

typedef struct Metadata {
    PageFrame *frames; // array of frames
    int curCounter;    // used by FIFO/LRU/CLOCK to set their counter
                       // FIFO: curCounter maintains the count of memory accesses
                       // LRU: curCounter maintains the list of pinning
                       // CLOCK: this is simply the index of the current frame we're looking at

    // add statistics here
    int numRead;
    int numWrite;
} Metadata;
/*
 * Creates a new buffer pool for an existing page file
 *  New Buffer Pool bp
 *      bp->numpages = numpages
 *      bp->strategy = strategy
 *      handles the page pageFileName
 *      Page Frames should be empty
 *      PageFile should already exist and be open
 *      stratData would be used for [EC] replacement strategies. Not necessary atm.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){
    bm->pageFile = pageFileName;
    bm->numPages = numPages;

    bm->strategy = strategy;

    Metadata *m = malloc(sizeof(struct Metadata));
    m->frames = malloc(sizeof(PageFrame) * numPages);
    m->numRead = 0;
    m->numWrite = 0;

    // init the pageframes as empty
    for(int i = 0; i < bm->numPages; i++){
        m->frames[i].frame = (BM_PageHandle){NO_PAGE, NULL};
        m->frames[i].dirty = FALSE;
        m->frames[i].fixcount = 0;
        m->frames[i].counter = -1;
        if(strategy == RS_LRU_K)
            m->frames[i].accesses = calloc((int)stratData, sizeof(int));
    }
    if(strategy == RS_LRU_K)
        m->curCounter = (int)stratData;
    else
        m->curCounter = 1;
    bm->mgmtData = m;
    return RC_OK;
}
/*
 * kills the given buffer pool, and ensures all resources are freed/closed
 *  Throw error if any pages are pinned
 *  call forceFlushPool
 *  Free the memory given to pages
 */
RC shutdownBufferPool(BM_BufferPool *const bm){
    Metadata *meta = bm->mgmtData;
    PageFrame *pages = meta->frames;
    // verify no pages are pinned
    for(int i = 0; i < bm->numPages; i++)
        if (pages[i].fixcount > 0)
            return RC_WRITE_FAILED; // None of the errors describe this failure

    // write all dirty pages
    if (forceFlushPool(bm) != RC_OK)
        return RC_WRITE_FAILED;
    // free the page data
    for(int i = 0; i < bm->numPages; i++) {
        if (pages[i].frame.data) // else it was never used and thus malloc'd, so we can't free.
            free(pages[i].frame.data);
    }
    free(pages);
    free(bm->mgmtData);
    return RC_OK;
}
/*
 * Writes all dirty pages in the buffer pool to disk
 *  Only write pages if fixcount = 0
 *  Marks the disk'd pages clean again.
 *  use forcePage
 */
RC forceFlushPool(BM_BufferPool *const bm){
    Metadata *meta = bm->mgmtData;
    PageFrame *pages = meta->frames;

    for(int i = 0; i < bm->numPages; i++){
        if (pages[i].fixcount == 0 && pages[i].dirty == TRUE){
            if(forcePage(bm, &pages[i].frame) != RC_OK)
                return RC_WRITE_FAILED;
            pages[i].dirty = FALSE;
        }
    }
    return RC_OK;
}
/*
 * Finds a page given the pagenum
 * ideally we'd have a hashmap or something, but for now its just a simple array
 */
PageFrame* findPage(BM_BufferPool *const bm, PageNumber pageNum){
    Metadata *meta = bm->mgmtData;
    PageFrame *pages = meta->frames;

    for(int i = 0; i < bm->numPages; i++)
        if(pages[i].frame.pageNum == pageNum)
            return &pages[i];
    return NULL;
}
/*
 * dumb max-heap implementation
 */
void updateLRU_K(Metadata *const meta, PageFrame *page, PageNumber counter){
    int maxK = meta->curCounter;
    int *arr = page->accesses;

    // iterate from n ... 1
    // move n - 1 to n
    for(int i = maxK - 1; i > 1; i--)
        arr[i] = arr[i - 1];
    arr[0] = counter;
}

// Buffer Manager Interface Access Pages
/*
 * marks the page as dirty
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    PageFrame *p = findPage(bm, page->pageNum);
    if(!p)
        return RC_WRITE_FAILED;
    p->dirty = TRUE;
    return RC_OK;
}
/*
 * unpins the page
 *  use page->pagenum to figure out which page to unpin
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    PageFrame *p = findPage(bm, page->pageNum);
    if(!p || p->fixcount <= 0)
        return RC_WRITE_FAILED;
    p->fixcount--;
    return RC_OK;
}
/*
 * Writes the current page to disk
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    Metadata *meta = bm->mgmtData;
    SM_FileHandle fh;
    if(openPageFile(bm->pageFile, &fh) != RC_OK)
        return RC_WRITE_FAILED;
    if(writeBlock(page->pageNum, &fh, page->data) != RC_OK)
        return RC_WRITE_FAILED;

    PageFrame *p = findPage(bm, page->pageNum);
    if(!p)
        return RC_WRITE_FAILED;
    p->dirty = FALSE;

    meta->numWrite++;
    if(closePageFile(&fh) != RC_OK)
        return RC_WRITE_FAILED;
    return RC_OK;
}

RC setupNewPage(BM_BufferPool *bm,
                  PageFrame *frame,
                  BM_PageHandle *const page,
                  const PageNumber pageNum
){


    Metadata *meta = bm->mgmtData;
    meta->numRead++;

    SM_FileHandle fh;
    if (openPageFile(bm->pageFile, &fh) != RC_OK)
        return RC_WRITE_FAILED;
    if(frame->frame.data)
        free(frame->frame.data); // wipe out the old data
    page->data = (char *) malloc(PAGE_SIZE); // and in with the new
    if (ensureCapacity(pageNum+1, &fh) != RC_OK)
        return RC_WRITE_FAILED;  // in case the client just wants to write a new page
    if (readBlock(pageNum, &fh, page->data) != RC_OK)
        return RC_WRITE_FAILED;
    if(closePageFile(&fh) != RC_OK)
        return RC_WRITE_FAILED;
    page->pageNum = pageNum;

    // add the page to our buffer pool
    frame->fixcount = 1;
    frame->frame = *page;

    if(bm->strategy == RS_CLOCK)
        frame->counter = 1;
    else if (bm->strategy == RS_LRU_K)
        updateLRU_K(meta, frame, meta->curCounter);
    else
        frame->counter = meta->curCounter;
    return RC_OK;
}

RC CLOCK(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    Metadata *meta = (Metadata *) bm->mgmtData;
    PageFrame *cur;

    // from frames[cur] to frames[ejectable], we go through and set the counter to 0
    // until we find a useable page.
    int i = meta->curCounter;
    int fullruns = 0;

    while(true) {
        cur = &meta->frames[i];
        if(cur->fixcount == 0 && cur->counter == 0){
            if(cur->dirty == TRUE)
                forcePage(bm, &cur->frame);
            setupNewPage(bm, cur, page, pageNum);
            meta->curCounter = i; // update the curPointer to the replaced page
            return RC_OK;
        }

        cur->counter = 0;

        i = (i + 1) % bm->numPages;
        // we might do a full run if every page started with counter = 1, and got set to 0
        // but if we do a second full run, then every page must be fixed > 0, which means we can't
        // eject anything more.
        if(i == meta->curCounter && ++fullruns >= 2)
            break;
    }

    return RC_WRITE_FAILED;
}


/*
 * pins the page
 *  use page->pagenum to figure out which page to pin in bufferpool
 *  page->data = bufferpool->pages[pagenum]->data
 * if the page doesn't already exist in the bufferpool, then we need to add it using the replacement strategy
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    Metadata *meta = bm->mgmtData;
    PageFrame *pages = meta->frames;
    PageFrame *minPage = &meta->frames[0];

    // realistically, this entire iteration should be skippable by a hashmap
    // but it's too much work to implement.

    // first check if the page already exists in the pool
    for(int i = 0; i < bm->numPages; i++){
        // if we already have the page, we can just give it to the client.
        if(pages[i].frame.pageNum == pageNum){
            pages[i].fixcount++;
            page->pageNum = pageNum;
            page->data = pages[i].frame.data;
            // The only place LRU is different from FIFO: It's counter is updated when re-pinned.
            switch(bm->strategy){
                case RS_LRU:   pages[i].counter = ++meta->curCounter; break;
                case RS_LRU_K: updateLRU_K(meta, &pages[i], ++meta->curCounter); break;
                case RS_LFU:   pages[i].counter++; break;
                case RS_CLOCK:
                    pages[i].counter = 1;
                    meta->curCounter = i;
                    break;
                default: break;
            }
            return RC_OK;
        }

        // we don't have the page currently, but we have space for a new page
        if(pages[i].frame.data == NULL) {
            setupNewPage(bm, &pages[i], page, pageNum);
            if(bm->strategy == RS_CLOCK)
                meta->curCounter = i;
            return RC_OK;
        }

        if(minPage->fixcount != 0)
            minPage = &pages[i];

        // the FIFO/LRU/LFU page to replace
        if (pages[i].fixcount == 0){
            int maxK = meta->curCounter;
            if (bm->strategy == RS_LRU_K && pages[i].accesses[maxK] < minPage->accesses[maxK])
                minPage = &pages[i];
            else if (pages[i].counter < minPage->counter)
                minPage = &pages[i];
        }
    }
    if(minPage->fixcount != 0) // no page was unpinned; client error.
        return RC_WRITE_FAILED;


    // now we're adding a new page, so FIFO/LIFO
    if(bm->strategy == RS_FIFO || bm->strategy == RS_LRU || bm->strategy == RS_LRU_K)
        meta->curCounter++; // we'll use the next value for new pages



    // since we don't have a free page, we'll have to use the replacement strategy to find a new one
    switch(bm->strategy){

        // FIFO/LRU/LFU/LRU_K set their counter's to the max of frame-list, so their logic for a new page is exactly the same
        case RS_FIFO:
        case RS_LRU:
        case RS_LRU_K:
        case RS_LFU:
            if(minPage->dirty)
                forcePage(bm, &minPage->frame);
            if (setupNewPage(bm, minPage, page, pageNum) != RC_OK)
                return RC_WRITE_FAILED;
            break;
        case RS_CLOCK:
            // CLOCK can't be embedded in the above loop, because it needs to set all that it passes through to 1
            // when it wants to replace. So if we embedded it above, it'd set it to 1 even if we already had the page.
            // Instead, it has its own circular list to work with.
            if (CLOCK(bm, page, pageNum) != RC_OK)
                return RC_WRITE_FAILED;
            break;

        default:
            break;
    }


    return RC_OK;
}

// Statistics Interface
/*
 * returns an array of PageNumber
 *  i'th element is the pageNum of the i'th page stored in pageframe
 *  empty page frame returns NO_PAGE
 */
PageNumber *getFrameContents (BM_BufferPool *const bm){
    PageNumber *p = malloc( bm->numPages * sizeof(PageNumber));
    Metadata *m = bm->mgmtData;
    PageFrame *pages = m->frames;
    for (int i = 0; i < bm->numPages; i++) {
        p[i] = pages[i].frame.pageNum;
    }

    return p;
}
/*
 * returns array of bools
 *  i'th element is TRUE if i'th page is dirty.
 *  else FALSE
 * empty pages are considered clean.
 */
bool *getDirtyFlags (BM_BufferPool *const bm){
    bool *p = malloc(bm->numPages * sizeof(bool));
    Metadata *m = bm->mgmtData;
    PageFrame *pages = m->frames;

    for(int i = 0; i < bm->numPages; i++){
        p[i] = pages[i].dirty;
    }

    return p;
}
/*
 * returns array of ints
 *  i'th element is the fixcount of the i'th page
 *  fixcount = 0 for empty page frames
 */
int *getFixCounts (BM_BufferPool *const bm){
    int *p = malloc(bm->numPages * sizeof(int));
    Metadata *m = bm->mgmtData;
    PageFrame *pages = m->frames;

    for (int i=0; i<bm->numPages; i++)
        p[i] = pages[i].fixcount;

    return p;
}
/*
 * number of reads by buffer pool since initialization
 *  code should update counter whenever reading new page from disk
 */
int getNumReadIO (BM_BufferPool *const bm){
    Metadata *meta = bm->mgmtData;
    return meta->numRead;
}
/*
 * number of writes to disk by buffer pool since initialization
 */
int getNumWriteIO (BM_BufferPool *const bm){
    Metadata *meta = bm->mgmtData;
    return meta->numWrite;
}
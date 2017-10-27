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
    struct PageFrame *next; // for use by the CLOCK alg; circular
    struct PageFrame *prev; // for use by the CLOCK alg; circular
} PageFrame;

typedef struct Metadata {
    PageFrame *frames; // array of frames
    PageFrame *cur;    // current position in the buffer
    int curCounter;    // used by FIFO/LRU to set their counter
                       // FIFO: curCounter maintains the count of memory accesses
                       // LRU: curCounter maintains the list of pinning
    int numRead;
    int numWrite;
    // add statistics here
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
        m->frames[i].next = NULL;
        m->frames[i].prev = NULL;
    }
    m->cur = &m->frames[0];
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
    for(int i = 0; i < bm->numPages; i++)
        if(pages[i].frame.data) // else it was never used and thus malloc'd, so we can't free.
            free(pages[i].frame.data);
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

PageFrame* findPage(BM_BufferPool *const bm, PageNumber pageNum){
    Metadata *meta = bm->mgmtData;
    PageFrame *pages = meta->frames;

    for(int i = 0; i < bm->numPages; i++)
        if(pages[i].frame.pageNum == pageNum)
            return &pages[i];
    return NULL;
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
    if (ensureCapacity(page->pageNum+1, &fh) != RC_OK)
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
    else
        frame->counter = meta->curCounter;

    // maintain the circular buffer for CLOCK
    frame->prev = meta->cur;
    meta->cur->next = frame;
    meta->cur = frame;
    return RC_OK;
}
RC CLOCK(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    Metadata *meta = (Metadata *) bm->mgmtData;
    PageFrame *cur = meta->cur;

    // if meta->cur is usable, we'll just use that and not move
    // else, we'll circle around till we find something that is useable.
    if(cur->fixcount == 0 && cur->counter == 0){
        if(cur->dirty == TRUE)
            forcePage(bm, &cur->frame);
        setupNewPage(bm, cur, page, pageNum);
        return RC_OK;
    }
    else
        cur = cur->next;
    // cur shouldn't find NULL because that means we had an free pageframe that had never been used
    // which should have been caught earlier by pinPage
    while(cur != meta->cur){
        if(cur == NULL)
            return RC_WRITE_FAILED;
        if(cur->fixcount == 0 && cur->counter == 0){
            meta->cur = cur;
            if(cur->dirty == TRUE)
                forcePage(bm, &cur->frame);
            setupNewPage(bm, cur, page, pageNum);
            return RC_OK;
        }
        cur->counter = 0;
        cur = cur->next;
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
    PageFrame *minPage = &pages[0];


    // first check if the page already exists in the pool
    for(int i = 0; i < bm->numPages; i++){
        // if we already have the page, we can just give it to the client.
        if(pages[i].frame.pageNum == pageNum){
            pages[i].fixcount++;
            page->pageNum = pageNum;
            page->data = pages[i].frame.data;
            // The only place LRU is different from FIFO: It's counter is updated when re-pinned.
            if(bm->strategy == RS_LRU)
                pages[i].counter = ++meta->curCounter;
            if(bm->strategy == RS_CLOCK)
                pages[i].counter = 1;
            return RC_OK;
        }

        // we don't have the page currently, but we have space for a new page
        if(pages[i].frame.data == NULL) {
            setupNewPage(bm, &pages[i], page, pageNum);
            return RC_OK;
        }

        // the FIFO/LRU page to replace
        if(pages[i].fixcount == 0 && pages[i].counter < minPage->counter)
            minPage = &pages[i];
    }


    // now we're adding a new page, so FIFO/LIFO
    if(bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
        meta->curCounter++; // we'll use the next value for new pages



    // since we don't have a free page, we'll have to use the replacement strategy to find a new one
    switch(bm->strategy){
        // FIFO/LRU set their counter's to the max of frame-list, so their logic for a new page is exactly the same
        case RS_FIFO:
        case RS_LRU:
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
        case RS_LFU:
            break;
        case RS_LRU_K:
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
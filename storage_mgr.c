//
// Created by Sharul on 9/28/17.
//

#include "storage_mgr.h"
#include "stdio.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
void initStorageManager(void) {

}

RC createPageFile(char *fileName) {
    FILE *p = fopen(fileName, "w");
    const char c_size[PAGE_SIZE] = {'\0'};

    if (!p)
        return RC_FILE_NOT_FOUND;
    if (!fwrite(c_size, sizeof(char), PAGE_SIZE, p))
        return RC_WRITE_FAILED;
    if (fclose(p))
        return RC_FILE_NOT_FOUND;
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *p = fHandle->mgmtInfo = fopen(fileName, "r+");

    if (!p) {
        strerror(errno);
        return RC_FILE_NOT_FOUND;
    }

    long int cur = ftell(p);
    fHandle->fileName = fileName;
    fHandle->curPagePos = (int) (ftell(p) / PAGE_SIZE);
    fseek(p, 0, SEEK_END);
    fHandle->totalNumPages = (int) (ftell(p) / PAGE_SIZE);
    fseek(p, cur, SEEK_SET);

    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fclose(fHandle->mgmtInfo) != 0)
        return RC_FILE_NOT_FOUND;

    fHandle->mgmtInfo = NULL; // so we don't try to re-close it or use after closed.

    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    if (remove(fileName) != 0)
        return RC_FILE_NOT_FOUND;
    return RC_OK;
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    FILE *fp = fHandle->mgmtInfo;
    if (!fHandle || !fp)
        return RC_FILE_HANDLE_NOT_INIT;
    if (pageNum < 0 || pageNum > fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;

    //finally, read the block
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return RC_READ_NON_EXISTING_PAGE;
    if (fread(memPage, sizeof(char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1; // the read failed somehow

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}





/* writing blocks to a page file */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    int RC;
    FILE* fp = fHandle->mgmtInfo;

    if(!fHandle || !fp)
        return RC_FILE_HANDLE_NOT_INIT;
    if (pageNum < 0)
        return RC_WRITE_FAILED;

    if ((RC = ensureCapacity(pageNum, fHandle)) != RC_OK) // for if pageNum > totalPageNum
        return RC;

    if (fseek(fp, (PAGE_SIZE * pageNum), SEEK_SET) != 0)
        return RC_WRITE_FAILED;

    if (fwrite(memPage, sizeof(char), PAGE_SIZE, fp) != PAGE_SIZE)
        return RC_WRITE_FAILED;

    fHandle->curPagePos = pageNum;

    return RC_OK;

}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    SM_PageHandle addon = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

    if (!addon)
        return -1; // Could not allocate memory

    if (fseek(fHandle->mgmtInfo, 0, SEEK_END) == 0) {
        if (fwrite(addon, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo)) {
            fHandle->totalNumPages++;
            return RC_OK;
        }
    }

    return RC_WRITE_FAILED;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    int RC;
    while (fHandle->totalNumPages <= numberOfPages)
        if ((RC = appendEmptyBlock(fHandle)) != RC_OK)
            return RC;
    return RC_OK;
}
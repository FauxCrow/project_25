package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here

        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
       
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs // Read the specified page from disk and throw exception if page doesnt exist
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        
        if (pid.getTableId() != getId()){
            throw new IllegalArgumentException();
        }

        if (pid.getPageNumber() >= this.numPages()) { //if page number out of range
            throw new IllegalArgumentException();
        }

        int pageSize = BufferPool.getPageSize();
        long offset = (long) pid.getPageNumber() * pageSize;

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            if (offset + pageSize > randomAccessFile.length()) {
                throw new IllegalArgumentException("Page does not exist in this file.");
            }

            byte[] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.readFully(data);

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
    

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pageNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        long offset = (long) pageNo * pageSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")){
            raf.seek(offset);
            raf.write(page.getPageData());
        }
        
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // return (int) this.file.length() / Database.getBufferPool().getPageSize();
        return (int) this.file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        List<Page> arrList = new ArrayList<>();
        HeapPage heapPage;
        for(int i = 0; i < numPages(); i++){
            PageId pid = new HeapPageId(getId(), i);
            heapPage = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots() > 0){
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                writePage(heapPage);
                arrList.add(heapPage);
                return arrList;
            }
        }
        
        HeapPageId newPid = new HeapPageId(getId(), numPages());
        byte[] newData = HeapPage.createEmptyPageData();
        HeapPage newPage = new HeapPage(newPid, newData);
        writePage(newPage);

        heapPage = (HeapPage) bufferPool.getPage(tid, newPid, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        heapPage.markDirty(true, tid);
        arrList.add(heapPage);
        return arrList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> arrList = new ArrayList<>();
        BufferPool bufferPool = Database.getBufferPool();
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid,rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        arrList.add(page);        
        return arrList;
        // not necessary for lab1
    }


    //using abstractDbFileIterator instead of DbfileIterator - cuz easier no need to manually implement hasNext() as its done for us in tat class
    //HeapFileIterator extends AbstractDbFileIterator and is used to iterate through all tuples in a DbFile.
    public class HeapFileIterator extends AbstractDbFileIterator {
        private int pageIndex;
        private final TransactionId tid;
        private final int tableId;
        private final int pageNum;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(int tableId, TransactionId tid, int pageNum) {
            this.tid = tid;
            this.tableId = tableId;
            this.pageNum = pageNum;
            pageIndex = 0;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator == null) //if iterator is null, return nothing
                return null;

            if (iterator.hasNext()) //if current page has tuples, return the next one
                return iterator.next();


            //As long as  haven’t reached the last page, keep moving to the next one
            while (pageIndex < pageNum - 1) {
                pageIndex++;
                //open(); //load next page

                PageId pid = new HeapPageId(tableId, pageIndex); //get next page
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); //load page
                iterator = page.iterator(); //get iterator for new page
                if (iterator == null)
                    return null;
                if (iterator.hasNext())
                    return iterator.next();
            }

            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageIndex = 0;
            PageId pid = new HeapPageId(tableId, pageIndex);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); //fetch page from disk
            iterator = page.iterator();
        }

        @Override
        public void close() {
            super.close();
            iterator = null;
            pageIndex = 0;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here 
        return new HeapFileIterator(this.getId(), tid, this.numPages());
    }

    
    

}


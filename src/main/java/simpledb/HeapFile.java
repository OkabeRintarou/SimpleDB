package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
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
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageOffset = pid.pageNumber();
        if(pageOffset >= numPages()){
            throw new IllegalArgumentException();
        }
        try{
            RandomAccessFile raf = new RandomAccessFile(file,"r");
            raf.seek(pageOffset * BufferPool.PAGE_SIZE);
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            raf.readFully(data);
            raf.close();
            HeapPageId hpid = new HeapPageId(pid.getTableId(),pid.pageNumber());
            return new HeapPage(hpid,data);
        }
        catch(IOException e){
            System.out.println("IOException : " + e.getMessage());
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        return (int)((file.length() + BufferPool.PAGE_SIZE - 1)/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        final TransactionId transactionId = tid;
        DbFileIterator iterator = new DbFileIterator() {

            private int pageOffset = 0;
            private boolean isOpen = false;
            private Iterator<Tuple> tupleIterator;
            private int tableId = getId();

            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                PageId pid = new HeapPageId(tableId,0);
                Page page = Database.getBufferPool().getPage(transactionId,pid,Permissions.READ_WRITE);
                tupleIterator = ((HeapPage)page).iterator();
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(isOpen){
                    if(tupleIterator.hasNext()){
                        return true;
                    }else{
                        if(pageOffset < numPages() - 1){
                            return true;
                        }else{
                            return false;
                        }
                    }
                }else{
                    return false;
                }
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(isOpen){
                    if(tupleIterator.hasNext()){
                        return tupleIterator.next();
                    }else if(pageOffset < numPages() - 1){
                        pageOffset++;
                        PageId pid = new HeapPageId(tableId,pageOffset);
                        Page page = Database.getBufferPool().getPage(transactionId,pid,Permissions.READ_WRITE);
                        tupleIterator = ((HeapPage)page).iterator();
                        return tupleIterator.next();
                    }else{
                        throw new NoSuchElementException();
                    }
                }else{
                    throw new NoSuchElementException();
                }
            }

            public void rewind() throws DbException, TransactionAbortedException {
                pageOffset = 0;
                PageId pid = new HeapPageId(tableId,pageOffset);
                tupleIterator = ((HeapPage)Database.getBufferPool().getPage(transactionId,pid,Permissions.READ_WRITE)).iterator();
            }

            public void close() {
                isOpen = false;
                pageOffset = 0;
                tupleIterator = null;
            }
        };
        return iterator;
    }

}


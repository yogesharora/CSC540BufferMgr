/*  File BufMgr,java */
 
package bufmgr;
 
import diskmgr.Page;
import exceptions.BufMgrException;
import exceptions.BufferPoolExceededException;
import exceptions.DiskMgrException;
import exceptions.HashEntryNotFoundException;
import exceptions.HashOperationException;
import exceptions.InvalidBufferException;
import exceptions.InvalidFrameNumberException;
import exceptions.InvalidReplacerException;
import exceptions.PageNotFoundException;
import exceptions.PageNotReadException;
import exceptions.PagePinnedException;
import exceptions.PageUnpinnedException;
import exceptions.ReplacerException;
import global.AbstractBufMgr;
import global.AbstractBufMgrFrameDesc;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
 
// *****************************************************
 
/**
 * This is a dummy buffer manager class. You will need to replace it with a
 * buffer manager that reads from and writes to disk
 * 
 * algorithm to replace the page.
 */
public class BufMgr extends AbstractBufMgr {
	// Replacement policies to be implemented
	public static final String Clock = "Clock";
	public static final String LRU = "LRU";
	public static final String MRU = "MRU";
 
	// Total number of buffer frames in the buffer pool. */
	private int numBuffers;
	private Map<PageId, BufMgrFrameDesc> pageTable = new Hashtable<PageId, BufMgrFrameDesc>();
	private byte[][] buffer;
	private BufMgrFrameDesc[] frameTable;
	private List<Integer> emptyList;
 
	/**
	 * Create a buffer manager object.
	 * 
	 * @param numbufs
	 *            number of buffers in the buffer pool.
	 * @param replacerArg
	 *            name of the buffer replacement policy (e.g. BufMgr.Clock).
	 * @throws InvalidReplacerException
	 */
	public BufMgr(int numbufs, String replacerArg)
			throws InvalidReplacerException {
		numBuffers = numbufs;
		setReplacer(replacerArg);
		init();
		((BufMgrReplacer)replacer).setBufferManager(this);
	}
 
	private void init() {
		buffer = new byte[numBuffers][MINIBASE_PAGESIZE];
		frameTable = new BufMgrFrameDesc[numBuffers];
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = new byte[MINIBASE_PAGESIZE];
		}
	}
 
	/**
	 * Default Constructor Create a buffer manager object.
	 * 
	 * @throws InvalidReplacerException
	 */
	public BufMgr() throws InvalidReplacerException {
		numBuffers = 1;
		replacer = new Clock(this);
		init();
	}
 
	/**
	 * Check if this page is in buffer pool, otherwise find a frame for this
	 * page, read in and pin it. Also write out the old page if it's dirty
	 * before reading. If emptyPage==TRUE, then actually no read is done to
	 * bring the page in.
	 * 
	 * @param pin_pgid
	 *            page number in the minibase.
	 * @param page
	 *            the pointer poit to the page.
	 * @param emptyPage
	 *            true (empty page); false (non-empty page)
	 * 
	 * @exception ReplacerException
	 *                if there is a replacer error.
	 * @exception HashOperationException
	 *                if there is a hashtable error.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception InvalidFrameNumberException
	 *                if there is an invalid frame number .
	 * @exception PageNotReadException
	 *                if a page cannot be read.
	 * @exception BufferPoolExceededException
	 *                if the buffer pool is full.
	 * @exception PagePinnedException
	 *                if a page is left pinned .
	 * @exception BufMgrException
	 *                other error occured in bufmgr layer
	 * @exception IOException
	 *                if there is other kinds of I/O error.
	 */
 
	public void pinPage(PageId pageId, Page page, boolean emptyPage)
			throws ReplacerException, HashOperationException,
			PageUnpinnedException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, IOException {
 
		BufMgrFrameDesc frame = pageTable.get(pageId);
		byte[] frameData;
		if (frame != null) {
			// page is already loaded
			returnPageInfo(page, frame);
		} else {
			// page has to be loaded
 
				// we need a victim frame
				int victimFrameNo;
				try
				{
					victimFrameNo = replacer.pick_victim();
				}
				catch(BufferPoolExceededException e)
				{
					throw new BufferPoolExceededException(null,"BufMgr::pinPage pick_victim failed");
				}
 
				BufMgrFrameDesc victimFrame = frameTable[victimFrameNo];
 
 
 
				if (victimFrame!=null && victimFrame.isDirty()) {
					try {
						flushPage(victimFrame.getPageNo());
					} catch (PageNotFoundException e) {
						throw new BufMgrException(e,
								"BufrMgr::pinPage: page number not found in flushPage");
					}
				}
 
				frameData = buffer[victimFrameNo];
 
				frameTable[victimFrameNo] = new BufMgrFrameDesc(pageId,
				frameData, victimFrameNo);
 
				frame = frameTable[victimFrameNo];
 
				// delete the entry from pageTable
 
				if(victimFrame!=null)
				{
					pageTable.remove(victimFrame.getPageNo());
				}
				createPageTableEntry(pageId, page, emptyPage, frame);
		}
	}
 
	private void createPageTableEntry(PageId pageId, Page page,
			boolean emptyPage, BufMgrFrameDesc frame)
			throws InvalidFrameNumberException, PageNotReadException {
		returnPageInfo(page, frame);
		loadPageFromDisk(pageId, page, emptyPage);
		pageTable.put(new PageId(pageId.getPid()), frame);
	}
 
	private void loadPageFromDisk(PageId pageId, Page page, boolean empty)
			throws PageNotReadException {
		if (!empty) {
			try {
				SystemDefs.JavabaseDB.read_page(pageId, page);
			} catch (Exception e) {
				throw new PageNotReadException(e,
						"BufrMgr::pinPage: DB_READ_PAGE_ERROR");
			}
		}
	}
 
	private void returnPageInfo(Page page, BufMgrFrameDesc frame)
			throws InvalidFrameNumberException {
		frame.pin();
		replacer.pin(frame.getFrameNumber());
		page.setpage(frame.getData());
	}
 
	/**
	 * To unpin a page specified by a pageId. If pincount>0, decrement it and if
	 * it becomes zero, put it in a group of replacement candidates. if
	 * pincount=0 before this call, return error.
	 * 
	 * @param globalPageId_in_a_DB
	 *            page number in the minibase.
	 * @param dirty
	 *            the dirty bit of the frame
	 * 
	 * @exception ReplacerException
	 *                if there is a replacer error.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception InvalidFrameNumberException
	 *                if there is an invalid frame number .
	 * @exception HashEntryNotFoundException
	 *                if there is no entry of page in the hash table.
	 */
	public void unpinPage(PageId pageId, boolean dirty)
			throws ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException {
 
		BufMgrFrameDesc frame = pageTable.get(pageId);
 
		if (frame != null) {
			int pinCount = frame.getPinCount();
			if (pinCount == 0) {
				throw new PageUnpinnedException(null,
						"BufrMgr::unPinPage: page to be unpinned is already unpinned");
			} else {
				frame.unpin();
				if (frame.getPinCount() == 0)
					replacer.unpin(frame.getFrameNumber());
			}
 
			frame.setDirtybit(dirty);
		} else {
			throw new HashEntryNotFoundException(null,
					"BufrMgr::unPinPage: page to be unpinned not loaded");
		}
	}
 
	/**
	 * Call DB object to allocate a run of new pages and find a frame in the
	 * buffer pool for the first page and pin it. If buffer is full, ask DB to
	 * deallocate all these pages and return error (null if error).
	 * 
	 * @param firstpage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 * @return the first page id of the new pages.
	 * 
	 * @exception BufferPoolExceededException
	 *                if the buffer pool is full.
	 * @exception HashOperationException
	 *                if there is a hashtable error.
	 * @exception ReplacerException
	 *                if there is a replacer error.
	 * @exception HashEntryNotFoundException
	 *                if there is no entry of page in the hash table.
	 * @exception InvalidFrameNumberException
	 *                if there is an invalid frame number.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception PagePinnedException
	 *                if a page is left pinned.
	 * @exception PageNotReadException
	 *                if a page cannot be read.
	 * @exception IOException
	 *                if there is other kinds of I/O error.
	 * @exception BufMgrException
	 *                other error occured in bufmgr layer
	 * @exception DiskMgrException
	 *                other error occured in diskmgr layer
	 */
	public PageId newPage(Page firstpage, int howmany)
			throws BufferPoolExceededException, HashOperationException,
			ReplacerException, HashEntryNotFoundException,
			InvalidFrameNumberException, PagePinnedException,
			PageUnpinnedException, PageNotReadException, BufMgrException,
			DiskMgrException, IOException {
 
		PageId newPageId = new PageId();
 
		try {
			SystemDefs.JavabaseDB.allocate_page(newPageId, howmany);
		} catch (Exception e) {
			throw new DiskMgrException(e,
					"BUFMGR::newPage() failed during allocating disk page");
		}
 
		try {
			pinPage(newPageId, firstpage, true);
		} catch (Exception e) {
			try {
				SystemDefs.JavabaseDB.deallocate_page(newPageId, howmany);
			} catch (Exception e1) {
				throw new DiskMgrException(e, "BUFMGR::newPage() failed " +
						"after pinPage failed and deallocate page failed");
			}
			throw new DiskMgrException(e,
					"BUFMGR::newPage() failed during pinPage");
		}
 
		return newPageId;
	}
 
	/**
	 * User should call this method if s/he needs to delete a page. this routine
	 * will call DB to deallocate the page.
	 * 
	 * @param globalPageId
	 *            the page number in the data base.
	 * @exception InvalidBufferException
	 *                if buffer pool corrupted.
	 * @exception ReplacerException
	 *                if there is a replacer error.
	 * @exception HashOperationException
	 *                if there is a hash table error.
	 * @exception InvalidFrameNumberException
	 *                if there is an invalid frame number.
	 * @exception PageNotReadException
	 *                if a page cannot be read.
	 * @exception BufferPoolExceededException
	 *                if the buffer pool is already full.
	 * @exception PagePinnedException
	 *                if a page is left pinned.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception HashEntryNotFoundException
	 *                if there is no entry of page in the hash table.
	 * @exception IOException
	 *                if there is other kinds of I/O error.
	 * @exception BufMgrException
	 *                other error occured in bufmgr layer
	 * @exception DiskMgrException
	 *                other error occured in diskmgr layer
	 */
	public void freePage(PageId pageId) throws InvalidBufferException,
			ReplacerException, HashOperationException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException,
			PageUnpinnedException, HashEntryNotFoundException, BufMgrException,
			DiskMgrException, IOException {
 
		BufMgrFrameDesc frame = pageTable.get(pageId);
 
		if (frame != null) {
			int pinCount = frame.getPinCount();
 
			if (pinCount > 1) {
				throw new PagePinnedException(null,
						"BufrMgr::freePage: page to be freed not loaded");
			} else {
				if(pinCount==1)
				{
					frame.unpin();
					replacer.unpin(frame.getFrameNumber());
				}
				// remove from pagetable/frametable
				pageTable.remove(pageId);
 
				// add it to empty list
				frameTable[frame.getFrameNumber()] = null;
				replacer.free(frame.getFrameNumber());
 
				// free it on disk
				try {
					SystemDefs.JavabaseDB.deallocate_page(pageId);
				} catch (Exception e1) {
					throw new DiskMgrException(e1, "BUFMGR::freepage failed " +
							"after pinPage failed and deallocate page failed");
				}
 
			}
 
		} else {
			// free it on disk
			try {
				SystemDefs.JavabaseDB.deallocate_page(pageId);
			} catch (Exception e1) {
				throw new DiskMgrException(e1, "BUFMGR::freepage failed");
			}
		}
	}
 
	/**
	 * Added to flush a particular page of the buffer pool to disk
	 * 
	 * @param pageid
	 *            the page number in the database.
	 * 
	 * @exception HashOperationException
	 *                if there is a hashtable error.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception PagePinnedException
	 *                if a page is left pinned.
	 * @exception PageNotFoundException
	 *                if a page is not found.
	 * @exception BufMgrException
	 *                other error occured in bufmgr layer
	 * @exception IOException
	 *                if there is other kinds of I/O error.
	 */
	public void flushPage(PageId pageId) throws HashOperationException,
			PageUnpinnedException, PagePinnedException, PageNotFoundException,
			BufMgrException, IOException {
 
		BufMgrFrameDesc frame = pageTable.get(pageId);
 
		if (frame != null) {
			if (frame.isDirty()) {
				try {
					SystemDefs.JavabaseDB.write_page(pageId, new Page(frame.getData()));
				} catch (Exception e) {
					throw new BufMgrException(e,
						"BufrMgr::flushPage: page cant be freed by diskmanager");
				}
			}
 
			frame.setDirtybit(false);
 
			if(frame.getPinCount()>0)
			{
				throw new PagePinnedException(null,
					"BufrMgr::flushPage: page is still pinned");
			}
 
		} else {
			throw new PageNotFoundException(null,
				"BufrMgr::flushPage: page to be flushed not loaded");
		}
	}
 
	/**
	 * Flushes all pages of the buffer pool to disk
	 * 
	 * @exception HashOperationException
	 *                if there is a hashtable error.
	 * @exception PageUnpinnedException
	 *                if there is a page that is already unpinned.
	 * @exception PagePinnedException
	 *                if a page is left pinned.
	 * @exception PageNotFoundException
	 *                if a page is not found.
	 * @exception BufMgrException
	 *                other error occured in bufmgr layer
	 * @exception IOException
	 *                if there is other kinds of I/O error.
	 */
	public void flushAllPages() throws HashOperationException,
			PageUnpinnedException, PagePinnedException, PageNotFoundException,
			BufMgrException, IOException {
 
		for (Iterator<PageId> iterator = pageTable.keySet().iterator(); iterator.hasNext();) {
			PageId pageId = (PageId) iterator.next();
			flushPage(pageId);
		}
	}
 
	/**
	 * Gets the total number of buffers.
	 * 
	 * @return total number of buffer frames.
	 */
	public int getNumBuffers() {
		return numBuffers;
	}
 
	/**
	 * Gets the total number of unpinned buffer frames.
	 * 
	 * @return total number of unpinned buffer frames.
	 */
	public int getNumUnpinnedBuffers() {
		int unpinned = 0;
		for (int i = 0; i < frameTable.length; i++) {
			BufMgrFrameDesc frame = frameTable[i];
			if (frame != null && frame.getPinCount() == 0) {
				unpinned++;
			} else if (frame == null) {
				unpinned++;
			}
		}
		return unpinned;
	}
 
	/** A few routines currently need direct access to the FrameTable. */
	public AbstractBufMgrFrameDesc[] getFrameTable() {
		return this.frameTable;
	}
 
}
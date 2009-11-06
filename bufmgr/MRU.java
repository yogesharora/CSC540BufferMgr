package bufmgr;

import exceptions.BufferPoolExceededException;
import exceptions.InvalidFrameNumberException;
import exceptions.PagePinnedException;
import exceptions.PageUnpinnedException;
import global.AbstractBufMgr;

import java.util.LinkedList;
import java.util.List;

/**
 * This class should implement a Clock replacement strategy.
 */
public class MRU extends BufMgrReplacer {

	List<Integer> evictionList = new LinkedList<Integer>();

	public MRU() {

	}

	public MRU(AbstractBufMgr b) {
		setBufferManager(b);
	}

	/**
	 * Pins a candidate page in the buffer pool.
	 * 
	 * @param frameNo
	 *            frame number of the page.
	 * @throws InvalidFrameNumberException
	 *             if the frame number is less than zero or bigger than number
	 *             of buffers.
	 * @return true if successful.
	 */
	public void pin(int frameNo) throws InvalidFrameNumberException {

		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"MRU::pin Invalid frame Number");
		}

		evictionList.remove(new Integer(frameNo));
	}

	/**
	 * Unpins a page in the buffer pool.
	 * 
	 * @param frameNo
	 *            frame number of the page.
	 * @throws InvalidFrameNumberException
	 *             if the frame number is less than zero or bigger than number
	 *             of buffers.
	 * @throws PageUnpinnedException
	 *             if the page is originally unpinned.
	 * @return true if successful.
	 */
	public boolean unpin(int frameNo) throws InvalidFrameNumberException,
			PageUnpinnedException {
		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"MRU::pin Invalid frame Number");
		}

		evictionList.remove(new Integer(frameNo));
		evictionList.add(0, new Integer(frameNo));

		return true;
	}

	/**
	 * Frees and unpins a page in the buffer pool.
	 * 
	 * @param frameNo
	 *            frame number of the page.
	 * @throws PagePinnedException
	 *             if the page is pinned.
	 */
	public void free(int frameNo) throws PagePinnedException {
		evictionList.remove(new Integer(frameNo));
	}

	/** Must pin the returned frame. */
	public int pick_victim() throws BufferPoolExceededException,
			PagePinnedException {

		if (evictionList.size() != 0) {
			Integer victim = evictionList.get(0);
			evictionList.remove(0);
			return victim.intValue();
		} else {
			throw new BufferPoolExceededException(null,
					"MRU:pic_victim buffer pool exceeded");
		}

	}

	/** Retruns the name of the replacer algorithm. */
	public String name() {
		return "MRU";
	}

	/**
	 * Counts the unpinned frames (free frames) in the buffer pool.
	 * 
	 * @returns the total number of unpinned frames in the buffer pool.
	 */
	public int getNumUnpinnedBuffers() {
		return evictionList.size();
	}
}

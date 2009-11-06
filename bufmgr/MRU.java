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

	List<Integer> emptyList = new LinkedList<Integer>();
	List<Integer> evictionList = new LinkedList<Integer>();
	boolean is_init = false;

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

		init();

		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"MRU::pin Invalid frame Number");
		}
		// No need to remove from anyList bcoz
		// this function is called from PinPage
		// if Page already exists in buffer then buffer is not in any list
		// else if a victim is chosen ..then buffer is removed from the
		// appropriate list
		// in the pick_victim function
		if (this.state_bit[frameNo] == Available) {
			emptyList.remove(new Integer(frameNo));
		} else if(this.state_bit[frameNo] == Referenced) {
			evictionList.remove(new Integer(frameNo));
		}
		this.state_bit[frameNo] = Pinned;
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

		init();

		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"MRU::pin Invalid frame Number");
		}

		if (this.state_bit[frameNo] == Pinned) {
			this.state_bit[frameNo] = Referenced;
			evictionList.add(0, new Integer(frameNo));
		}
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

		init();

		// Page must be already in the evictionList
		if (this.state_bit[frameNo] == Referenced) {
			evictionList.remove(new Integer(frameNo));
		}
		
		emptyList.add(0, new Integer(frameNo));
		this.state_bit[frameNo] = Available;
	}

	/** Must pin the returned frame. */
	public int pick_victim() throws BufferPoolExceededException,
			PagePinnedException {

		int victim;
		init();

		if (emptyList.size() > 0) {
			victim = emptyList.remove(0).intValue();
		} else if (evictionList.size() > 0) {
			victim = evictionList.remove(0).intValue();
		} else {
			throw new BufferPoolExceededException(null,
					"MRU:pic_victim buffer pool exceeded");
		}
		return victim;
	}

	/** Retruns the name of the replacer algorithm. */
	public String name() {
		init();
		return "MRU";
	}

	/**
	 * Counts the unpinned frames (free frames) in the buffer pool.
	 * 
	 * @returns the total number of unpinned frames in the buffer pool.
	 */
	public int getNumUnpinnedBuffers() {
		return (evictionList.size() + emptyList.size());
	}

	public void init() {
		if (!is_init) {
			for (int i = 0; i < mgr.getNumBuffers(); i++) {
				emptyList.add(new Integer(i));
			}
			is_init = true;
		}
	}
}
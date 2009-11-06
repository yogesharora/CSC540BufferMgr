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
public class Clock extends BufMgrReplacer {

	List<Integer> evictionList = new LinkedList<Integer>();
	List<Boolean> evictionFlag = new LinkedList<Boolean>();
	int hand;

	public Clock() {
		hand = 0;
	}

	public Clock(AbstractBufMgr b) {
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
					"CLOCK::pin Invalid frame Number");
		}

		removeFromEvictionList(frameNo);
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
					"CLOCK::pin Invalid frame Number");
		}

		int index = evictionList.indexOf(new Integer(frameNo));
		if (index > 0) {
			evictionFlag.set(index, new Boolean(true));
		} else {
			evictionList.add(new Integer(frameNo));
			evictionFlag.add(new Boolean(true));
		}
		return true;
	}

	private void removeFromEvictionList(int frameNo) {

		int index = evictionList.indexOf(new Integer(frameNo));
		if (index > 0) {
			evictionList.remove(index);
			evictionFlag.remove(index);
		}
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
		removeFromEvictionList(frameNo);
	}

	/** Must pin the returned frame. */
	public int pick_victim() throws BufferPoolExceededException,
			PagePinnedException {

		if (evictionList.size() != 0) {
			while (true) {
				boolean flag = evictionFlag.get(hand).booleanValue();
				if (flag == true) {
					evictionFlag.set(hand, new Boolean(false));
					hand++;
					if (hand >= evictionFlag.size())
						hand = 0;
				} else {
					int victim = evictionList.get(hand).intValue();
					evictionList.remove(hand);
					evictionFlag.remove(hand);
					return victim;
				}
			}
		} else {
			throw new BufferPoolExceededException(null,
					"CLOCK:pick_victim buffer pool exceeded");
		}

	}

	/** Retruns the name of the replacer algorithm. */
	public String name() {
		return "CLOCK";
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

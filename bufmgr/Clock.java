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
 
	List<Integer> allBufferList = new LinkedList<Integer>();
	List<Boolean> referenceFlag = new LinkedList<Boolean>();
	int hand;
	boolean is_init=false;
 
	public Clock() {
 
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
 
		if(!is_init)
		{
			init_CLOCK();
		}
		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"CLOCK::pin Invalid frame Number");
		}
 
		if(this.state_bit[frameNo]==Available || this.state_bit[frameNo]==Referenced)
		{
			this.state_bit[frameNo]=Pinned;
		}
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
 
		if(!is_init)
		{
			init_CLOCK();
		}
 
		// find the frame and remove it
		if (frameNo < 0 || frameNo > mgr.getNumBuffers()) {
			throw new InvalidFrameNumberException(null,
					"CLOCK::pin Invalid frame Number");
		}
 
		this.state_bit[frameNo]=Referenced;
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
		if(!is_init)
		{
			init_CLOCK();
		}
		this.state_bit[frameNo]=Available;
	}
 
	/** Must pin the returned frame. */
	public int pick_victim() throws BufferPoolExceededException,
			PagePinnedException {
 
		if(!is_init)
		{
			init_CLOCK();
		}
		for (int i = hand; i < 2* mgr.getNumBuffers(); i++) {
 
			hand=i%mgr.getNumBuffers();
			if(this.state_bit[hand]==Available)
			{
				return(hand);
			}
			else
			{
				if(this.state_bit[hand]==Referenced)
				{
					if(this.frameTable[hand].getPinCount()==0)
					{
						if(referenceFlag.get(hand).booleanValue()==false)
						{
							return(hand);
						}
						else
						{
							referenceFlag.set(hand, new Boolean(false));
						}
					}
				}
			}
		}
 
 
		throw new BufferPoolExceededException(null,
		"CLOCK:pick_victim buffer pool exceeded");	
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
		if(!is_init)
		{
			init_CLOCK();
		}
 
		int count = 0;
		for (int i = 0; i < this.frameTable.length; i++) {
			if(this.state_bit[i]!=Pinned)
			{
				count++;
			}			
		}
		return count;
	}
 
	void init_CLOCK()
	{
		hand = 0;
		for (int i = 0; i < mgr.getNumBuffers(); i++) {
			allBufferList.add(new Integer(i));
			referenceFlag.add(new Boolean(true));
		}	
		is_init=true;
	}
}
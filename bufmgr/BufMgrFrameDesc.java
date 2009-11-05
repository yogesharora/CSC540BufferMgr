package bufmgr;

import global.GlobalConst;
import global.PageId;

public class BufMgrFrameDesc extends global.AbstractBufMgrFrameDesc implements
		GlobalConst {
	
	int pinCount;
	boolean dirtyBit;
	PageId pageId;
	private byte[] data;
	private int frameNumber;
	
	BufMgrFrameDesc(PageId page, byte[] data, int frameNumber) {
		super();
		pinCount = 0;
		dirtyBit = false;
		pageId = new PageId(page.getPid());
		this.data = data;
		this.frameNumber = frameNumber;
	}

	/**
	 * Returns the pin count of a certain frame page.
	 * 
	 * @return the pin count number.
	 */
	public int getPinCount() {
		return (pinCount);
	};

	/**
	 * Increments the pin count of a certain frame page when the page is pinned.
	 * 
	 * @return the incremented pin count.
	 */
	public int pin() {
		System.out.println("BufMgrFrame::pin");
		return (++pinCount);
	}

	/**
	 * Decrements the pin count of a frame when the page is unpinned. If the pin
	 * count is equal to or less than zero, the pin count will be zero.
	 * 
	 * @return the decremented pin count.
	 */
	public int unpin() {
		System.out.println("BufMgrFrame::unpin");
		if (pinCount > 0) {
			--pinCount;
		}
		
		return (pinCount);
	}

	public PageId getPageNo() {
		return pageId;
	}

	public boolean isDirty() {
		return dirtyBit;
	}

	public void setDirtybit(boolean dirty) {
		dirtyBit = dirty;
	}

	public void setPincount(int pin) {
		pinCount = pin;
	}

	public void setPageNo(PageId pageToBringIn) {
		pageId = pageToBringIn;
	}

	public byte[] getData() {
		return data;
	}

	public int getFrameNumber() {
		return frameNumber;
	}
}

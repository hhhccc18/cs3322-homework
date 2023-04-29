package simpledb.util;

import simpledb.BufferPool.PageItem;
import simpledb.Page;
import simpledb.PageId;

import java.util.HashMap;

@Deprecated
public class LRUHelper {
//	private final int capacity;
	private final LRULinkedList<PageItem> list = new LRULinkedList<>();
	private final HashMap<PageItem,
			LRULinkedList<PageItem>.Node> nodeMap = new HashMap<>();
	
	public LRUHelper() {
//		this.capacity = capacity;
	}
	
	public void add(PageItem item) {
		assert(!nodeMap.containsKey(item));
		
		LRULinkedList<PageItem>.Node node = list.pushBack(item);
		nodeMap.put(item, node);
	}
	
	public void remove(PageItem item) {
		assert(nodeMap.containsKey(item));
		
		LRULinkedList<PageItem>.Node node = nodeMap.get(item);
		list.remove(node);
		nodeMap.remove(item);
	}
	
	public void touch(PageItem item) {
		assert(nodeMap.containsKey(item));
		
		LRULinkedList<PageItem>.Node node = nodeMap.get(item);
		list.moveToFront(node);
	}
	
	public int size() {
		return list.length();
	}
	
	public PageItem peek() {
		return list.peek();
	}
	
	public PageItem peekAndRemove() {
		PageItem item = this.peek();
		this.remove(item);
		return item;
	}
}

package simpledb.util;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Deprecated
public class LRULinkedList<T> {
	// I wrote this for the sake of performance
	// But later I found I am baka
	// Since LinkedHashMap solves everything
	
	public class Node {
		public Node next;
		public Node prev;
		public T data;
		
		public Node(T data) {
			this.data = data;
		}
		
		public Node(T data, Node prev, Node next) {
			this.data = data;
			this.prev = prev;
			this.next = next;
		}
		
		boolean isSentinel() {
			return this == LRULinkedList.this.sentinel;
		}
	}
	
	public final Node sentinel;
	private int count = 0;
	
	public LRULinkedList() {
		this.sentinel = new Node(null);
		this.sentinel.prev = this.sentinel;
		this.sentinel.next = this.sentinel;
	}
	
	public int length() {
		return this.count;
	}
	
	public T peek() {
		return this.front().data;
	}
	
	public Node front() {
		return this.sentinel.next;
	}
	
	public Node back() {
		return this.sentinel.prev;
	}
	
	public void insertBefore(Node node, Node o) {
		o.prev = node.prev;
		o.next = node;
		
		node.prev.next = o;
		node.prev = o;
	}
	
	public Node insertBefore(Node node, T data) {
		Node o = new Node(data);
		this.count++;
		this.insertBefore(node, o);
		
		return o;
	}
	
	public Node pushBack(T data) {
		return this.insertBefore(this.sentinel, data);
	}
	
	public void moveToFront(Node o) {
		assert(!o.isSentinel());
		
		o.prev.next = o.next;
		o.next.prev = o.prev;
		
		this.insertBefore(this.sentinel.next, o);
	}
	
	public void remove(Node o) {
		assert(!o.isSentinel());
		
		o.prev.next = o.next;
		o.next.prev = o.prev;
		o.data = null;
		
		this.count--;
	}
	
	public void popFront() {
		assert(this.length() > 0);
		this.remove(this.front());
	}
	
	public void clear() {
//		while(this.length() > 0)
//			this.popFront();
		
		this.sentinel.prev = this.sentinel;
		this.sentinel.next = this.sentinel;
		this.count = 0;
		// Someone told me that JVM GC is smart enough to handle this
	}
	
	public Stream<Node> nodeStream() {
		return Stream.iterate(this.front(), o -> o.next)
				.limit(this.length());
	}
	
	public Stream<T> dataStream() {
		return this.nodeStream().map(o -> o.data);
	}
	
	public boolean contains(T data) {
		return this.dataStream().anyMatch(o -> o.equals(data));
	}
}

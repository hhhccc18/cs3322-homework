package simpledb.utils;

import java.util.Objects;

public class HashablePair<S, T> {
	public S first;
	public T second;
	
	public HashablePair(S first, T second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof HashablePair))
			return false;
		
		HashablePair<?, ?> o = (HashablePair<?, ?>)obj;
		return this.first.equals(o.first) && this.second.equals(o.second);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(this.first, this.second);
	}
}

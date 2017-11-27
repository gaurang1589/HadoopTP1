
class StringAndInt implements Comparable<StringAndInt>{
		private String tag;
		private int occurence;
		
		
		
		public StringAndInt(String tag, int occurence) {
			super();
			this.tag = tag;
			this.occurence = occurence;
		}

		@Override
		public int compareTo(StringAndInt o) {
			
			return Integer.compare(occurence, o.getOccurence());
		}

		public String getTag() {
			return tag;
		}

		public void setTag(String tag) {
			this.tag = tag;
		}

		public int getOccurence() {
			return occurence;
		}

		public void setOccurence(int occurence) {
			this.occurence = occurence;
		}
		
	}
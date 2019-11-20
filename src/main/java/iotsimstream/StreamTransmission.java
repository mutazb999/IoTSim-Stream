package iotsimstream;

/**
 * This class represents transmission of a Stream.
 * It controls amount of data transmitted in a shared data medium. Relation between
 * Transmission and Channel is the same as Cloudlet and CloudletScheduler,
 * but here we consider only the time shared case, representing a shared
 * channel among different simultaneous Stream transmissions
 */
public class StreamTransmission {
	double totalLength; 	/*length in MB*/
        int sourceDatacenterId;
        int sourceVMId;
        int destinationDatacenterId;	
	int destinationVMId;
	double leftLength;
	Stream stream;
	
	public StreamTransmission(Stream stream,int sourceDatacenterId, int sourceVMId, int destinationDatacenterId, int destinationVMId) {
		this.sourceDatacenterId=sourceDatacenterId;
                this.sourceVMId = sourceVMId;
                this.destinationDatacenterId=destinationDatacenterId;
		this.destinationVMId = destinationVMId;
		this.stream = stream;
		this.totalLength = stream.getSize();
		this.leftLength = totalLength;
	}
	
	/**
	 * Sums some amount of data to the already transmitted data
	 * @param completed amount of data completed since last update
	 */
	public void addCompletedLength(double completed){
		leftLength-=completed;
		if (leftLength<0.1) leftLength = 0.0;
	}

    public int getSourceDatacenterId() {
        return sourceDatacenterId;
    }

    public int getDestinationDatacenterId() {
        return destinationDatacenterId;
    }
	
        
	public int getSourceVMId(){
		return sourceVMId;
	}
	
	public int getDestinationVMId(){
		return destinationVMId;
	}
	
	public Stream getstream() {
		return stream;
	}
	
	public double getLength(){
		return leftLength;
	}
	
	/**
	 * Says if the DataItem transmission finished or not.
	 * @return true if transmission finished; false otherwise
	 */
	public boolean isCompleted(){
		return leftLength<0.1;
	}
}

package iotsimstream;

/**
 * This class contains information necessary to enable stream
 * transfer among virtual machines in a datacenter or in different datacenters. It contains
 * the Stream being transferred, datacenter origin, VM origin, datacenter destination and VM destination.
 * 
 * @author Mutaz Barika
 */
public class TransferStreamEvent {

	Stream Stream;
        int sourceDatacenterId;
	int sourceVMId;
        int destinationDatacenterId;
        int destinationVMId;
	
	public TransferStreamEvent(Stream stream, int sourceDatacenterId, int sourceVMId, int destinationDatacenterId, int destinationVMId) {
		this.Stream = stream;
                this.sourceDatacenterId=sourceDatacenterId;
		this.sourceVMId = sourceVMId;
                this.destinationDatacenterId=destinationDatacenterId;
		this.destinationVMId = destinationVMId;
	}

	public Stream getStream() {
		return Stream;
	}

    public int getSourceDatacenterId() {
        return sourceDatacenterId;
    }

    public int getDestinationDatacenterId() {
        return destinationDatacenterId;
    }

        
	public int getSourceVMId() {
		return sourceVMId;
	}

	public int getDestinationVMId() {
		return destinationVMId;
	}	
}

package iotsimstream;


/**
 * This class encapsulates a VM that has been provisioned. It contains
 * the actual VM, its start and end time, and the cost.
 */
public class ProvisionedSVm {
	
	SVM svm;
	long startTime;
	long endTime;
	double cost; //Unit: centers per second
        int datacenterID;
	
	public ProvisionedSVm(SVM svm, long startTime, long endTime, double cost, int datacenterID) {
		this.svm = svm;
		this.startTime = startTime;
		this.endTime = endTime;
		this.cost = cost;
                this.datacenterID=datacenterID;
	}
        
        public ProvisionedSVm(SVM svm, long startTime, long endTime, double cost) {
		this.svm = svm;
		this.startTime = startTime;
		this.endTime = endTime;
		this.cost = cost;
	}
	
	public SVM getVm() {
		return svm;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}
	
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	
	public double getCost(){
		return cost;
	}
}

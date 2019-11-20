package iotsimstream;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;

/**
 *
 * @author Mutaz Barika
 */
public class ServiceCloudlet extends Cloudlet{
    
    List<Stream> requiredInputStreams;
    List<Stream> producedOutputStreams;
    int serviceID;
    double serviceProcessingSize; // service processing requirement (in MI/MB)
    double relativeProcessingPercent; //it is a percentage of cloudlet processing to otherc cloudlets within a service. For example, if service has two cloudlets scheduled on two VMs, where VM1 has 2000MPIS and Vm2 has 4000MIPS so that the first cloudlet percentage is 2000/6000 and the second is 4000/6000.
    double totalOfProcessedStream;
    
    public ServiceCloudlet(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw, double serviceProcessingSize,int ownerid, int serviceID) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);
        if(this != null) this.setUserId(ownerid);
        requiredInputStreams=new ArrayList<Stream>();
        producedOutputStreams=new ArrayList<Stream>();
        this.serviceProcessingSize=serviceProcessingSize;
        this.serviceID=serviceID;
        this.totalOfProcessedStream=0.0;
    }
    
    public void addRequiredInputStream(Stream x)
    {
        requiredInputStreams.add(x);
    }
    
    public void addProducedOutputStream(Stream x)
    {
        producedOutputStreams.add(x);
    }

    public void setRelativeProcessingPercent(double relativeProcessingPercent) {
        this.relativeProcessingPercent = relativeProcessingPercent;
    }

    public double getRelativeProcessingPercent() {
        return relativeProcessingPercent;
    }

    public int getServiceID() {
        return serviceID;
    }
    
    
    
}

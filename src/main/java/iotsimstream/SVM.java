package iotsimstream;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;


/**
 *
 * @author Mutaz Barika
 */
public class SVM extends Vm{
    
    Queue<Stream> inputQueue;
    Queue<Stream> outputQueue;
    
    public SVM(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm, CloudletScheduler cloudletScheduler) {
        super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
        inputQueue=new LinkedList<>();
        outputQueue=new LinkedList<>();
    }
    
    public void addStreamToInputQueue(Stream s)
    {
        inputQueue.add(s);
    }
    
    public Stream getStreamFromInputQueue() throws InterruptedException
    {
        return inputQueue.remove();
    }
    
    public void addStreamToOutputQueue(Stream s)
    {
        outputQueue.add(s);
    }
    
    public Stream getStreamFromOutputQueue() throws InterruptedException
    {
        return outputQueue.remove();
    }

    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
        
        double time=super.updateVmProcessing(currentTime, mipsShare);
        
        //Check VM inputQueue, if there is/are stream , just dequeue them and put them in scheduler inputQueue
        while(!inputQueue.isEmpty())
        {
            try{       
                Stream s= getStreamFromInputQueue();
                ServiceCloudletSchedulerSpaceShared scheduler= (ServiceCloudletSchedulerSpaceShared) getCloudletScheduler();
                scheduler.addStreamToInputQueue(s);
            }catch(Exception ex){ex.printStackTrace();}
        }
        
        //Check CloudletScheduler outputQueue, there is/are streams, just dequeue them and put them in VM OutputQueue
        ServiceCloudletSchedulerSpaceShared scheduler= (ServiceCloudletSchedulerSpaceShared) getCloudletScheduler();
        while(!scheduler.outputQueue.isEmpty())
        {
            try{       
                Stream s= scheduler.outputQueue.remove();
                addStreamToOutputQueue(s);
            }catch(Exception ex){ex.printStackTrace();}
        }
        return time;
    } 
}

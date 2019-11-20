package iotsimstream;

import java.util.HashMap;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 *
 * @author Mutaz Barika
 */
public class ExternalSource extends SimEntity {

    
    public static final int SEND_STREAM = 111111;
    public static final int STOP_SENDING_STREAM = 111112;
    Stream stream;
    double datarate; //in MB/s
    boolean stopSendingStream;
    
    public ExternalSource(String name, int streamid, int ownerId, double datarate) {
        super(name);
        this.datarate=datarate;
        stream = new Stream(streamid, ownerId, getId(), "exsource", this.datarate);
        stopSendingStream=false;
    }

    @Override
    public void startEntity() {
        System.out.println(getName() + " is starting...");
        Log.printConcatLine(getName(), " is starting...");
    }

    @Override
    public void processEvent(SimEvent ev) {
        
        if (ev == null){
                Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Null event ignored.");
        } else {
                int tag = ev.getTag();
                switch(tag){
                        case SEND_STREAM: processSendStream(ev); break;
                        case STOP_SENDING_STREAM: processStopSendingStream(); break;
                        case CloudSimTags.END_OF_SIMULATION: break;
                        default: Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Unknown event ignored. Tag: "+tag);
                }
        }
    }

    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
    }
    
    private void processSendStream(SimEvent ev)
    {
        if( !stopSendingStream )
        {
            //Update the generation time of stream befroe sending
            stream.setStreamTime(CloudSim.clock());
            
            HashMap<Stream, Integer> streamPortionsVMMap= StreamSchedulingOnSVMs.getStreamPortionsSchedulingOnVMs(stream);
            
            for(Stream streamPortion: streamPortionsVMMap.keySet())
            {
                int vmid=streamPortionsVMMap.get(streamPortion);
                int datacenterid = StreamSchedulingOnSVMs.getDatacenterID(vmid);
                Object[] data=new Object[2];
                data[0]=vmid;
                data[1]=streamPortion; //stream portion
                
                //Send stream portion now
                sendNow(datacenterid, BigDatacenter.EXSOURCE_STREAM, data);
            }
            
            //Schedule next send
            schedule(getId(), 1, SEND_STREAM);
        }
    }
    
    private void processStopSendingStream()
    {
        stopSendingStream=true;
    }

    /*public int getServiceid() {
        return serviceid;
    }*/

    public Stream getStream() {
        return stream;
    }
    
    public double getDatarate() {
        return datarate;
    }
    
}

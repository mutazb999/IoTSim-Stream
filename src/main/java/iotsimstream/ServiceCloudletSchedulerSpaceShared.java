package iotsimstream;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.PriorityQueue;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import sun.misc.Queue;

/**
 * CloudletSchedulerSpaceShared implements a policy of scheduling performed by a virtual machine
 * to run its {@link ServiceCloudlet Cloudlets}.
 * It consider there will be only one ServiceCloudlet (cloudlet) per VM.
 * @author Mutaz Barika
 * @since IoTSim-Stream Toolkit 1.0
 */
public class ServiceCloudletSchedulerSpaceShared extends CloudletScheduler {
	/** The number of PEs currently available for the VM using the scheduler,
         * according to the mips share provided to it by
         * {@link #updateVmProcessing(double, java.util.List)} method. */
	protected int currentCpus;
        

	/** The number of used PEs. */
	protected int usedPes;
        //Queue<Stream> inputQueue;
        Queue<Stream> outputQueue;
        PriorityQueue<Stream> inputQueue;
        
        Hashtable<Integer, Stream> workingInputStreamMap;
        
        boolean startPC;  // for starting one processing cycle
        boolean waitingStreamsForNextPC; //for next processing cycle
        double totalInputSize;
        double totalOutputSize;
        
        ArrayList<Stream> assumeProcessedStreams; //this list is used to put streams that are assumed to be processed since the processing requirment for these streams are less than minimum time between events 
        
        
	/**
	 * Creates a new ServiceCloudletSchedulerSpaceShared object. This method must be invoked before
	 * starting the actual simulation.
	 * 
	 */
	public ServiceCloudletSchedulerSpaceShared() {
		super();
		usedPes = 0;
		currentCpus = 0;
                inputQueue=new PriorityQueue<>(); //we do not need to use any comprator since the stream class is implemented as comparable
                outputQueue=new Queue<>();
                workingInputStreamMap=new Hashtable<>();
                startPC=false;
                waitingStreamsForNextPC=true;
                totalInputSize=0;
                totalOutputSize=0;
                assumeProcessedStreams=new ArrayList<>();
	}

	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);
		double timeSpam = currentTime - getPreviousTime(); // time since last update
		double capacity = 0.0;
		int cpus = 0;
                
		for (Double mips : mipsShare) { // count the CPUs available to the VMM
                    
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}
		currentCpus = cpus;
		capacity /= cpus; // average capacity of each cpu
                
                
                // The only cloudlet in this scheudler is not running anymore (i.e. not in execution list). 
                //That's happen in case of the continous execution of this cloudlet is stopped by GraphAppEngine when simulation time is over
                //Therefore, the continuous execution is stopped and 0 is returned
		if (getCloudletExecList().size() == 0)// && getCloudletWaitingList().size() == 0) //no more cloudlets in this scheduler
                {
			setPreviousTime(currentTime);
			return 0.0;
		}
                
                //Check if status of cloudlet is success that means its continous running is being stopped by GraphAppEngine whhen the simulation time is over
                List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
		for (ResCloudlet rcl : getCloudletExecList()) {
			//interepted by GraphAppEngine, finished anyway
			if(rcl.getCloudletStatus() == Cloudlet.SUCCESS) {
				toRemove.add(rcl);
				cloudletFinish(rcl);
			}
		}
		getCloudletExecList().removeAll(toRemove);
                
                
		// each machine in the exec list has the same amount of cpu
		for (ResCloudlet rcl : getCloudletExecList()) 
                {
                    
                      ServiceCloudlet cl = (ServiceCloudlet) rcl.getCloudlet();
                    
                      //Cloudlet in execution status
                      if(startPC) //Cloudlet meet the processing requirement
                      {
                          rcl.updateCloudletFinishedSoFar(
                               (long) (capacity * timeSpam * rcl.getNumberOfPes() * Consts.MILLION));
                      }
                      
                     if(!waitingStreamsForNextPC)
                     {
                    

                            if (rcl.getRemainingCloudletLength() == 0) {

                                    //Set meetProcessingReq to false to prepare for next processing
                                    startPC=false;
                                    

                                    //Get output stream and add it to output stream in order to allow VM to eecongnize it in order to be send to corssponding dependency servcies
                                    
                                    for(int i=0;i<cl.producedOutputStreams.size();i++)
                                    {
                                        Stream tempStream=cl.producedOutputStreams.get(i);
                                        
                                        if(totalOutputSize==0) //calaculate for first time and the rest just using the value
                                            totalOutputSize=tempStream.getSize();
                                        
                                        
                                        if(totalInputSize==0) ////calaculate for first time and the rest just using the value
                                        {
                                            for(Stream s:cl.requiredInputStreams)
                                            {
                                                //Check processingType of this stream for this cloudlet
                                                if(s.getReplicaProcessing().contains(cl.getServiceID()))
                                                {
                                                    //Replica processing
                                                    totalInputSize+=s.getSize();
                                                }
                                                else if(s.getPartitionProcessing().containsKey(cl.getServiceID()))
                                                {
                                                    //Partition processing
                                                    double partitionPercentage=s.PartitionProcessing.get(cl.getServiceID());
                                                    totalInputSize+= s.getSize() * (partitionPercentage/100);
                                                }
                                                else //stream from external source
                                                    totalInputSize+=s.getSize();


           
                                            }
                                        }
                                        
                                           
                                        //Take the size of max processed portion from both workingInputStreamMap and assumeProcessedStreams
                                        double maxProcessedPortionsSize=0.0;
                                        for(Integer sid:workingInputStreamMap.keySet())
                                        {
                                            if(workingInputStreamMap.get(sid).getSize()>maxProcessedPortionsSize)
                                                maxProcessedPortionsSize=workingInputStreamMap.get(sid).getSize();
                                        }
                                        for(Stream s:assumeProcessedStreams)
                                        {
                                            if(s.getSize()>maxProcessedPortionsSize)
                                                maxProcessedPortionsSize=s.getSize();
                                        }
                                        
                                        //After getting max value multiply it by the number of processed streams
                                        double approxProcessedStreamPortionsSize= maxProcessedPortionsSize * (workingInputStreamMap.size()+ assumeProcessedStreams.size());
                                        
                                        double propotionOfOutputToInput=totalOutputSize/ totalInputSize;
                                        double outputSize= propotionOfOutputToInput * approxProcessedStreamPortionsSize;
                                        
                                        
                                        Stream outStream=new Stream(tempStream.getId(), tempStream.getOwnerId(), tempStream.getProducerid(),tempStream.getTypeOfProducer(), outputSize );
                                        outStream.setReplicaProcessing(tempStream.getReplicaProcessing());
                                        outStream.setPartitionProcessing(tempStream.getPartitionProcessing());
                                        outStream.setIsPortion(false);
                                        
                                        outputQueue.enqueue(outStream);

                                        //For print
                                        String processedStresms="(";
                                        for(Stream stream:assumeProcessedStreams)
                                            processedStresms+= "Stream#" + stream.getId()+ "- Portion#" + stream.getPortionID() + " , ";
                                        for(Integer sid:workingInputStreamMap.keySet())
                                            processedStresms+= "Stream#" + workingInputStreamMap.get(sid).getId()+ "- Portion#" + workingInputStreamMap.get(sid).getPortionID() + " , ";
                                        processedStresms=processedStresms.substring(0, processedStresms.length()-2) + ")";
                                        Log.printLine(CloudSim.clock()+": ServiceCloudlet#"+cl.getCloudletId()+" is processed Input Streams"+ processedStresms + " and produced Output Stream #"+cl.producedOutputStreams.get(0).getId());   
                                        cl.totalOfProcessedStream+=approxProcessedStreamPortionsSize;
                                    }

                                    //Clear all working streams to be ready for next processing 
                                    workingInputStreamMap.clear();
                                    waitingStreamsForNextPC=true;
                                    assumeProcessedStreams.clear();
                            }

                    }
                      
                 if(!inputQueue.isEmpty() && waitingStreamsForNextPC)
                  {
                      try{
                          boolean check=true;
                          
                          while(check && !inputQueue.isEmpty())
                          {
                              boolean continueCheck=false;
                              Stream s=inputQueue.peek();
                              if(!workingInputStreamMap.containsKey(s.getId())) //compare stream id not portion id since the required streams towards service is determined by stream ids
                              {
                                  s=inputQueue.poll();
                                  workingInputStreamMap.put(s.getId(), s);
                                  continueCheck=true;
                              }
                              
                              if(checkAllRequiredStreamsAvalaible(cl))
                              {
                                  //Get total size of streams in both lists
                                  double totalInputStremSize=0.0; // Unit: MB/s
                                  for(Integer streamID: workingInputStreamMap.keySet())
                                  {
                                    Stream stream = workingInputStreamMap.get(streamID);
                                    totalInputStremSize+=stream.getSize();
                                  }
                                  for(Stream stream: assumeProcessedStreams)
                                  {
                                    totalInputStremSize+=stream.getSize();
                                  }
                                  
                                  //Get reuqired MIPs for processing calculated total size
                                  double reqMIPSForProcessing= (cl.serviceProcessingSize * totalInputStremSize);
                                  
                                  //check if the time reuqired for procesisng the total size of streams in both lists is less than 0.1
                                  if(reqMIPSForProcessing/(capacity*cpus)< CloudSim.getMinTimeBetweenEvents())
                                  {
                                      for(Integer sid:workingInputStreamMap.keySet())
                                          assumeProcessedStreams.add(workingInputStreamMap.get(sid)); //assume that these streams are already processed since they can be processed in less than minimum time between events
                                      
                                      workingInputStreamMap.clear();
                                  }
                                  else
                                  {
                                      check=false;
                                  }
                              }                              
                              else if(!continueCheck)
                              {
                                  check=false;
                              }
                          }                            
                        }catch(Exception ex){ex.printStackTrace();}
                      }
                 
                 
                      if(checkAllRequiredStreamsAvalaible(cl) && waitingStreamsForNextPC)
                      {   
                            //Get length for this cloudlet based on the size of inpute streams
                            double totalInputStremSize=0.0; // Unit: MB/s
                            //Calculate total size of input streams
                            for(Integer streamID: workingInputStreamMap.keySet())
                            {
                                //Get stream
                                Stream s = workingInputStreamMap.get(streamID);
                                totalInputStremSize+=s.getSize();
                            }
                            
                            double actualLength= (cl.serviceProcessingSize * totalInputStremSize);
                            
                            if(cl.getCloudletLength() == 1) //First run and the length of cloudlet is one becuase initialization, so that we need to subtract one MI
                                cl.setCloudletLength(((long) (cl.getCloudletTotalLength() + (long)(actualLength)) / cpus)-1);
                            else
                                cl.setCloudletLength((long) (cl.getCloudletTotalLength() + (long)(actualLength)) / cpus);
                            
                            startPC=true;
                            waitingStreamsForNextPC=false;
                      }
		}

		
		// estimate time is Quantum value if cloudlet waits streams or estimate finish time of one processing cycle for this cloudlet in the execution queue
		double nextEvent = Double.MAX_VALUE;
                for (ResCloudlet rcl : getCloudletExecList()) {
                    //We cannot consider 0.1 (default min. time between events) here as this cloudlet may be not starting PC (i.e. in waiting stream phase), so it should check its input queue according to the value of updating datacenter network (quantum), where considering only 0.1 means propagating delay in fetching incoming streams
                    //Thus, we need to check if cloudlet will not be starting PC (waiting for inoming streams), the next event should be quantum value. Otherwise, cloudlet will start processing one PC so that it should calculate next event time
                    
                    double estimatedFinishTime=0;
                    
                    //This happen in case of such cloudlet cannot start new PC as it needs to wait for incoming streams (i.e it is in waiting stream phase), so that next event should be Quantum value (next time for updaing network)
                    if(startPC) //if(!waitingStreamsForNextPC)
                    {
			double remainingLength = rcl.getRemainingCloudletLength();
			estimatedFinishTime = currentTime + (remainingLength / (capacity * rcl.getNumberOfPes()));
                    }
                    else
                        estimatedFinishTime = currentTime + BigDatacenter.QUANTUM;
                    


                    if (estimatedFinishTime < nextEvent) {
                            nextEvent = estimatedFinishTime;
                    }
		}
		setPreviousTime(currentTime);
		return nextEvent;
	}

	@Override
	public Cloudlet cloudletCancel(int cloudletId) {
		// First, looks in the finished queue
		for (ResCloudlet rcl : getCloudletFinishedList()) {
			if (rcl.getCloudletId() == cloudletId) {
				getCloudletFinishedList().remove(rcl);
				return rcl.getCloudlet();
			}
		}

		// Then searches in the exec list
		for (ResCloudlet rcl : getCloudletExecList()) {
			if (rcl.getCloudletId() == cloudletId) {
				getCloudletExecList().remove(rcl);
				if (rcl.getRemainingCloudletLength() == 0) {
					cloudletFinish(rcl);
				} else {
					rcl.setCloudletStatus(Cloudlet.CANCELED);
				}
				return rcl.getCloudlet();
			}
		}

		// Now, looks in the paused queue
		for (ResCloudlet rcl : getCloudletPausedList()) {
			if (rcl.getCloudletId() == cloudletId) {
				getCloudletPausedList().remove(rcl);
				return rcl.getCloudlet();
			}
		}

		// Finally, looks in the waiting list
		for (ResCloudlet rcl : getCloudletWaitingList()) {
			if (rcl.getCloudletId() == cloudletId) {
				rcl.setCloudletStatus(Cloudlet.CANCELED);
				getCloudletWaitingList().remove(rcl);
				return rcl.getCloudlet();
			}
		}

		return null;

	}

	@Override
	public boolean cloudletPause(int cloudletId) {
		boolean found = false;
		int position = 0;

		// first, looks for the cloudlet in the exec list
		for (ResCloudlet rcl : getCloudletExecList()) {
			if (rcl.getCloudletId() == cloudletId) {
				found = true;
				break;
			}
			position++;
		}

		if (found) {
			// moves to the paused list
			ResCloudlet rgl = getCloudletExecList().remove(position);
			if (rgl.getRemainingCloudletLength() == 0) {
				cloudletFinish(rgl);
			} else {
				rgl.setCloudletStatus(Cloudlet.PAUSED);
				getCloudletPausedList().add(rgl);
			}
			return true;

		}

		// now, look for the cloudlet in the waiting list
		position = 0;
		found = false;
		for (ResCloudlet rcl : getCloudletWaitingList()) {
			if (rcl.getCloudletId() == cloudletId) {
				found = true;
				break;
			}
			position++;
		}

		if (found) {
			// moves to the paused list
			ResCloudlet rgl = getCloudletWaitingList().remove(position);
			if (rgl.getRemainingCloudletLength() == 0) {
				cloudletFinish(rgl);
			} else {
				rgl.setCloudletStatus(Cloudlet.PAUSED);
				getCloudletPausedList().add(rgl);
			}
			return true;

		}

		return false;
	}

	@Override
	public void cloudletFinish(ResCloudlet rcl) {
		rcl.setCloudletStatus(Cloudlet.SUCCESS);
		rcl.finalizeCloudlet();
		getCloudletFinishedList().add(rcl);
		usedPes -= rcl.getNumberOfPes();
	}

	@Override
	public double cloudletResume(int cloudletId) {
		boolean found = false;
		int position = 0;

		// look for the cloudlet in the paused list
		for (ResCloudlet rcl : getCloudletPausedList()) {
			if (rcl.getCloudletId() == cloudletId) {
				found = true;
				break;
			}
			position++;
		}

		if (found) {
			ResCloudlet rcl = getCloudletPausedList().remove(position);

			// it can go to the exec list
			if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) {
				rcl.setCloudletStatus(Cloudlet.INEXEC);
				for (int i = 0; i < rcl.getNumberOfPes(); i++) {
					rcl.setMachineAndPeId(0, i);
				}

				long size = rcl.getRemainingCloudletLength();
				size *= rcl.getNumberOfPes();
				rcl.getCloudlet().setCloudletLength(size);

				getCloudletExecList().add(rcl);
				usedPes += rcl.getNumberOfPes();

				// calculate the expected time for cloudlet completion
				double capacity = 0.0;
				int cpus = 0;
				for (Double mips : getCurrentMipsShare()) {
					capacity += mips;
					if (mips > 0) {
						cpus++;
					}
				}
				currentCpus = cpus;
				capacity /= cpus;

				long remainingLength = rcl.getRemainingCloudletLength();
				double estimatedFinishTime = CloudSim.clock()
						+ (remainingLength / (capacity * rcl.getNumberOfPes()));

				return estimatedFinishTime;
			} else {// no enough free PEs: go to the waiting queue
				rcl.setCloudletStatus(Cloudlet.QUEUED);

				long size = rcl.getRemainingCloudletLength();
				size *= rcl.getNumberOfPes();
				rcl.getCloudlet().setCloudletLength(size);

				getCloudletWaitingList().add(rcl);
				return 0.0;
			}

		}

		// not found in the paused list: either it is in in the queue, executing or not exist
		return 0.0;

	}

	@Override
	public double cloudletSubmit(Cloudlet cloudlet, double fileTransferTime) {
		// it can go to the exec list
		if ((currentCpus - usedPes) >= cloudlet.getNumberOfPes()) {
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.INEXEC);
			for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
				rcl.setMachineAndPeId(0, i);
			}
			getCloudletExecList().add(rcl);
			usedPes += cloudlet.getNumberOfPes();
		} else {// no enough free PEs: go to the waiting queue
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.QUEUED);
			getCloudletWaitingList().add(rcl);
			return 0.0;
		}

		// calculate the expected time for cloudlet completion
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : getCurrentMipsShare()) {
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}

		currentCpus = cpus;
		capacity /= cpus;

		// use the current capacity to estimate the extra amount of
		// time to file transferring. It must be added to the cloudlet length
		double extraSize = capacity * fileTransferTime;                
		long length = cloudlet.getCloudletLength();
		length += extraSize;
		cloudlet.setCloudletLength(length);
            
		return cloudlet.getCloudletLength() / capacity;
	}

	@Override
	public double cloudletSubmit(Cloudlet cloudlet) {
            
		return cloudletSubmit(cloudlet, 0.0);
	}

	@Override
	public int getCloudletStatus(int cloudletId) {
		for (ResCloudlet rcl : getCloudletExecList()) {
			if (rcl.getCloudletId() == cloudletId) {
				return rcl.getCloudletStatus();
			}
		}

		for (ResCloudlet rcl : getCloudletPausedList()) {
			if (rcl.getCloudletId() == cloudletId) {
				return rcl.getCloudletStatus();
			}
		}

		for (ResCloudlet rcl : getCloudletWaitingList()) {
			if (rcl.getCloudletId() == cloudletId) {
				return rcl.getCloudletStatus();
			}
		}

		return -1;
	}

	@Override
	public double getTotalUtilizationOfCpu(double time) {
		double totalUtilization = 0;
		for (ResCloudlet gl : getCloudletExecList()) {
			totalUtilization += gl.getCloudlet().getUtilizationOfCpu(time);
		}
		return totalUtilization;
	}

	@Override
	public boolean isFinishedCloudlets() {
		return getCloudletFinishedList().size() > 0;
	}

	@Override
	public Cloudlet getNextFinishedCloudlet() {
		if (getCloudletFinishedList().size() > 0) {
			return getCloudletFinishedList().remove(0).getCloudlet();
		}
		return null;
	}

	@Override
	public int runningCloudlets() {
		return getCloudletExecList().size();
	}

	/**
	 * Returns the first cloudlet to migrate to another VM.
	 * 
	 * @return the first running cloudlet
	 * 
         * @todo it doesn't check if the list is empty
	 */
	@Override
	public Cloudlet migrateCloudlet() {
		ResCloudlet rcl = getCloudletExecList().remove(0);
		rcl.finalizeCloudlet();
		Cloudlet cl = rcl.getCloudlet();
		usedPes -= cl.getNumberOfPes();
		return cl;
	}

	@Override
	public List<Double> getCurrentRequestedMips() {
		List<Double> mipsShare = new ArrayList<Double>();
		if (getCurrentMipsShare() != null) {
			for (Double mips : getCurrentMipsShare()) {
				mipsShare.add(mips);
			}
		}
		return mipsShare;
	}

	@Override
	public double getTotalCurrentAvailableMipsForCloudlet(ResCloudlet rcl, List<Double> mipsShare) {
                /*@todo The param rcl is not being used.*/
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : mipsShare) { // count the cpus available to the vmm
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}
		currentCpus = cpus;
		capacity /= cpus; // average capacity of each cpu
		return capacity;
	}

	@Override
	public double getTotalCurrentAllocatedMipsForCloudlet(ResCloudlet rcl, double time) {   
                //@todo the method isn't in fact implemented
		// TODO Auto-generated method stub
		return 0.0;
	}

	@Override
	public double getTotalCurrentRequestedMipsForCloudlet(ResCloudlet rcl, double time) {
                //@todo the method isn't in fact implemented
		// TODO Auto-generated method stub
		return 0.0;
	}

	@Override
	public double getCurrentRequestedUtilizationOfRam() {
                //@todo the method isn't in fact implemented
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getCurrentRequestedUtilizationOfBw() {
                //@todo the method isn't in fact implemented
		// TODO Auto-generated method stub
		return 0;
	}
        
        private boolean checkAllRequiredStreamsAvalaible(ServiceCloudlet cloudlet)
        {
            for(Stream s: cloudlet.requiredInputStreams)
            {
                
                if(!workingInputStreamMap.containsKey(s.getId()))
                //if(!workingInputStreamNumbers.contains(i))
                    return false;
            }
            
            return true;
        }
     
     public void addStreamToInputQueue(Stream stream)
     {
         stream.setArrivalTime(CloudSim.clock());
         inputQueue.offer(stream);
     }
}

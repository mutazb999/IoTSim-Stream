package iotsimstream.vmOffers;

import iotsimstream.Properties;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import org.cloudbus.cloudsim.Vm;

public class VmOffersDatacenter1 extends VMOffers {
	
	
        double baseMips=Double.parseDouble(Properties.MIPS_PERCORE.getProperty(0));
        long vmBw=1000;
	
	@Override
	public LinkedHashMap<Vm, Double> getVmOffers() {
		
		if(vmOffersTable.size()==0)
                {
                    //Note that price is in cents per second
                    vmOffersTable.put(new Vm(0,0, baseMips,2, 4096,vmBw,  8129,"",null), (double)  ((0.2*100)/3600)); //Small
                    vmOffersTable.put(new Vm(1,0, baseMips,4,  7168,vmBw,  16384,"",null), (double)  ((0.4*100)/3600)); //Medium
                    vmOffersTable.put(new Vm(2,0, baseMips,8,  14336,vmBw,  32768,"",null), (double)  ((0.6*100)/3600)); //Large
                    vmOffersTable.put(new Vm(3,0, baseMips,16,  30720,vmBw,  65536,"",null), (double)  ((0.8*100)/3600)); //XLarge
                }
                
		return vmOffersTable;
	}

        
        @Override
        public double getCost(double mips, int pes, int memory, long bw) {
            for(Vm vm: vmOffersTable.keySet())
            {
                if(vm.getMips() == mips
                        && vm.getNumberOfPes() == pes
                        && vm.getRam() == memory
                        && vm.getBw() == bw
                        )
                    return vmOffersTable.get(vm);
            }
            
            return 0;
        }
        
        
	@Override
	public long getBootTime() {
		return Long.parseLong(Properties.VM_DELAY.getProperty());
	}
}

package iotsimstream;

import java.util.Hashtable;
import java.util.LinkedHashMap;

import org.cloudbus.cloudsim.Vm;

public class VmOffersDatacenter2 extends VMOffers {
	
	double baseMips=Double.parseDouble(Properties.MIPS_PERCORE.getProperty(1));
        long vmBw=1000;
	
	@Override
	public LinkedHashMap<Vm, Double> getVmOffers() {
		
		if(vmOffersTable.size()==0)
                {
                    //Note that price is in cents per second
                    vmOffersTable.put(new Vm(0,0, baseMips,2,  4096,vmBw,  8192,"",null), (double)  ((0.4*100)/3600)); //Small
                    vmOffersTable.put(new Vm(1,0, baseMips,4,  8192,vmBw,  18432,"",null), (double)  ((0.8*100)/3600)); //Medium
                    vmOffersTable.put(new Vm(2,0, baseMips,8,  16384,vmBw,  34816,"",null), (double)  ((1.6*100)/3600)); //Large
                    vmOffersTable.put(new Vm(3,0, baseMips,16,  32768,vmBw,  69632,"",null), (double)  ((3.2*100)/3600)); //XLarge
                    //vmOffersTable.put(new Vm(3,0,4*baseMips,1,baseMem,0,baseStorage,"",null), baseCost);
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

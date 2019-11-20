package iotsimstream;

/**
 * This class supplies seeds for random numbers generators. This guarantees that all the
 * parameters have exactly the same seeds for a given simulation scenario, so they experience
 * similar conditions during execution.
 */

public class SeedGenerator {

    static long[] seeds = { 1040529 , 1310319};
    
    
    //Return fixed seed for each paramter, so that the same random number from a selected range will be returned across all scenarios
        public static long getSeedVMDelayGenerator() {
		return seeds[0];
	}
    
        public static long getSeedSchedulingPolicy() {
		return seeds[1];
	}
}

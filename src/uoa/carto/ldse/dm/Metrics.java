/**
 * 
 */
package uoa.carto.ldse.dm;

import jsc.contingencytables.ContingencyTable2x2;
import jsc.contingencytables.FishersExactTest;

import org.apache.commons.math3.distribution.HypergeometricDistribution;
import org.apache.commons.math3.stat.inference.ChiSquareTest;

/**
 * @author vkopsachilis
 *
 */
public class Metrics {
	
	public static double calculateJaccard(int datacells, int datbcells, int commoncells){
		return (double)commoncells/(datacells+datbcells-commoncells);	
	}
	
	public static double calculateOverlapCoef(int datacells, int datbcells, int commoncells){
		 return Math.round(((double)commoncells/(double)Math.min(datacells,datbcells))*10000.0)/100.0; //keep only two decimals
		// excl=excl*(1-((double)Math.max(datacells,datbcells)/extent)); //I dont remember why
	}
	
	public static double calculateHyperGeometry(int extent, int datacells, int datbcells, int commoncells){
		 HypergeometricDistribution hd=new  HypergeometricDistribution(extent,Math.max(datacells, datbcells),Math.min(datacells, datbcells));
		 return hd.upperCumulativeProbability(commoncells);
		
	}
	
	public static double calculateIndepedenceRatio(int extent, int datacells, int datbcells, int commoncells){
		 double pa=(double)datacells/extent;		 
		 double pb=(double)datbcells/extent;
		 double pc=(double)commoncells/extent;		 	 
		 return pc/(pa*pb);
	}
	
	public static double calculateX2(int extent, int datacells, int datbcells, int commoncells){	
		if (commoncells>5) {
			ChiSquareTest xs=new ChiSquareTest();
			long[][] values={{commoncells,datacells-commoncells},{datbcells-commoncells,extent-commoncells-datacells-datbcells}};
			return xs.chiSquareTest(values);
		 }else{
			return 9999.99;
		 }
		
	}
	
	public static double calculateFisher(int extent, int datacells, int datbcells, int commoncells){
		
		 if ((datacells-commoncells>0) && (datbcells-commoncells>0)){
			 ContingencyTable2x2 ct=new  ContingencyTable2x2(commoncells,datacells-commoncells,datbcells-commoncells,extent-commoncells-datacells-datbcells);
			 FishersExactTest ft=new FishersExactTest(ct);
		     return ft.getOneTailedSP();
		 } else{
			 return 9999.99;
		 }
	}

}

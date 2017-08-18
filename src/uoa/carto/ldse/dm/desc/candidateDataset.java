/**
 * 
 */
package uoa.carto.ldse.dm.desc;

/**
 * @author vkopsachilis
 *
 */
public class candidateDataset {
	
	 int datasetId;
	 int extent;
	 int datacells;
	 int datbcells;
	 int commoncells;
	 
	 
	  
	public candidateDataset(int datasetId, int extent, int datacells,
			int datbcells, int commoncells) {
		super();
		 
	    //fix for convex hull change
		if (datacells<commoncells) datacells=commoncells;
		if (datbcells<commoncells) datbcells=commoncells;
		//////////
		 
		this.datasetId = datasetId;
		this.extent = extent;
		this.datacells = datacells;
		this.datbcells = datbcells;
		this.commoncells = commoncells;
	}
	
	
	public int getDatasetId() {
		return datasetId;
	}
	public void setDatasetId(int datasetId) {
		this.datasetId = datasetId;
	}
	public int getExtent() {
		return extent;
	}
	public void setExtent(int extent) {
		this.extent = extent;
	}
	public int getDatacells() {
		return datacells;
	}
	public void setDatacells(int datacells) {
		this.datacells = datacells;
	}
	public int getDatbcells() {
		return datbcells;
	}
	public void setDatbcells(int datbcells) {
		this.datbcells = datbcells;
	}
	public int getCommoncells() {
		return commoncells;
	}
	public void setCommoncells(int commoncells) {
		this.commoncells = commoncells;
	}
	 
	 

}

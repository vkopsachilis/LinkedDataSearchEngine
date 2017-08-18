/**
 * 
 */
package uoa.carto.ldse.dm;

import java.sql.SQLException;
import java.util.List;

import uoa.carto.ldse.dm.desc.candidateDataset;
import uoa.carto.ldse.interfaces.DBInterface;

/**
 * @author vkopsachilis
 *
 */
public class DatasetMatcher {

	/**
	 * @param args
	 */
	public static String summariesTable="in_quadtree_summaries";
	public static String quadtreeTable="quadtree_05_global";
	public static String inputDatasetsTable="input_dataset_exp_prec";
	public static String testDatasetsTable="in_dataset_ontology_clean";
	public static String resultsTable="matcherresults";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
		
			DBInterface.initDBforDatasetMatcher(resultsTable); //Empty intersections
		
			List<Integer> inputDatasetsIds=DBInterface.getSampleDatasets(inputDatasetsTable);
		
			 for (int i=1; i<=inputDatasetsIds.size();i++){
				 System.out.println("Building Intersections Table for dataset "+ inputDatasetsIds.get(i-1));
				 DBInterface.buildIntersectionTableforDataset(inputDatasetsIds.get(i-1), summariesTable,quadtreeTable, inputDatasetsTable, testDatasetsTable);
				 List<candidateDataset> candidateDatasets = DBInterface.getCandidateDatasets();
				 System.out.println(candidateDatasets.size() + " candidate datasets found ");
				 for (int c=1; c<=candidateDatasets.size();c++){
					 if (!DBInterface.isDatasetPairAlreadyInDB(resultsTable,candidateDatasets.get(c-1).getDatasetId(),inputDatasetsIds.get(i-1))){
						 System.out.println(" Calculating metrics for candidate " + candidateDatasets.get(c-1).getDatasetId());
						 double jaccard=Metrics.calculateJaccard(candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 double overlap=Metrics.calculateOverlapCoef(candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 double hg=Metrics.calculateHyperGeometry(candidateDatasets.get(c-1).getExtent(),candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 double ir=Metrics.calculateIndepedenceRatio(candidateDatasets.get(c-1).getExtent(),candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 double x2=Metrics.calculateX2(candidateDatasets.get(c-1).getExtent(),candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 double fisher=Metrics.calculateFisher(candidateDatasets.get(c-1).getExtent(),candidateDatasets.get(c-1).getDatacells(), candidateDatasets.get(c-1).getDatbcells(), candidateDatasets.get(c-1).getCommoncells());
						 DBInterface.addResultsToDB(resultsTable,inputDatasetsIds.get(i-1), DBInterface.getDatasetNameById(inputDatasetsIds.get(i-1)),candidateDatasets.get(c-1).getDatasetId(),DBInterface.getDatasetNameById(candidateDatasets.get(c-1).getDatasetId()), candidateDatasets.get(c-1).getExtent(),candidateDatasets.get(c-1).getDatacells(),candidateDatasets.get(c-1).getDatbcells(),candidateDatasets.get(c-1).getCommoncells(),jaccard,overlap,hg,ir,x2,fisher);
					 }
				 }
				 DBInterface.emptyIntersectionsTable();
			 }
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
	} // End Main

	
	
	
} // End Class

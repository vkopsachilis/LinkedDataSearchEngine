package uoa.carto.ldse.dc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.QueryException;

import uoa.carto.ldse.interfaces.DBInterface;
import uoa.carto.ldse.interfaces.TriplesInterface;

/**
 * 
 */

/**
 * @author vkopsachilis
 *
 */
//Builds Summaries for the availiable Resources
public class SummariesBuilder {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	 
	public static String indexTable= "quadtree_05_global";  
	
	    
	public static void main(String[] args) throws SQLException {
		
		
		 DBInterface.initDBforSummariesBuilder();
		 try  {	
			
				 
			 List<Integer> datasetResourceIds=DBInterface.getDatasetResourceIds();
			 //Iterate Through Spatial Resources
			 for (int r=1; r<=datasetResourceIds.size();r++){	
					 String resource_url=DBInterface.getResourceUrlById(datasetResourceIds.get(r-1));
					 System.out.println("Parsing: "+resource_url);
					 List<String> resourceDatasets=DBInterface.getResourceDatasets(datasetResourceIds.get(r-1));				
					 List<Integer> resourceDatasetsIds=DBInterface.getResourceDatasetsIds(datasetResourceIds.get(r-1));
					 //Iterate Through A resource Datasets
					 for (int d=1; d<=resourceDatasets.size();d++){
						 System.out.println("Getting geometries from "+d+"th class " + resourceDatasetsIds.get(d-1)+ ":"+resourceDatasets.get(d-1) );
						 List<Integer> datasetOntologies=DBInterface.getDatasetOntologies(resourceDatasetsIds.get(d-1));
						 //Iterate Through A resource Datasets Ontologies
						 for (int o=1; o<=datasetOntologies.size();o++){
							System.out.println("Ontology: "+datasetOntologies.get(o-1));
							List<String> classGeometries=TriplesInterface.returnClassGeometries(resource_url,resourceDatasets.get(d-1),datasetOntologies.get(o-1));
							if (classGeometries!=null){
								DBInterface.AddGeometriesToTempCoordsTable(classGeometries);
								DBInterface.buildQuadtreeSummary(classGeometries.size(),DBInterface.getDatasetOntologyId(resourceDatasetsIds.get(d-1), datasetOntologies.get(o-1)));						
								DBInterface.emptyTempCoordsTable();
							}
						}
				}								 										
			 }//end for Spatial Resources 
			 
		 }catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);	 	 
		 }catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
		 }
				
	}//end main
		
	

}

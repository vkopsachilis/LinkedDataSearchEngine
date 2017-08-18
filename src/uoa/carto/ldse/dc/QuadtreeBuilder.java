package uoa.carto.ldse.dc;
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
//Builds a Quadtree Index based on the availiable datasets
public class QuadtreeBuilder {

	/**
	 * @param args
	 */
	
	public static int divisionCriterion=30;
	public static String indexTable= "quadtree_05_global_synch_div30";
	public static int scales= 9;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 try  {	
			 
			 DBInterface.initDBforQuadtreeBuilder();
			 
			 int currentScale=1;
			 //Iterate for 9 times (scales)
			 while (currentScale<scales){
			 
				 List<Integer> datasetResourceIds=DBInterface.getDatasetResourceIds();
				 //Iterate Through Spatial Resources
				 for (int r=1; r<=datasetResourceIds.size();r++){	
					// if ((datasetResourceIds.get(r-1)==351) || (datasetResourceIds.get(r-1)==393)){
						 //Dont parse dbpedia
					// }else{
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
								DBInterface.updateDatasetsInQuadtree();							
								DBInterface.emptyTempCoordsTable();
							}
			//			}
				}	
					 }
			 }//end for Spatial Resources
			
			 DBInterface.prepareNextQuadtreeScale();
			 currentScale++;
		  }//end while
		 
		}catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);
		}catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
		}

	}//End Main
	
		

}//End Class

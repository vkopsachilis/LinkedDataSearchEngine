package uoa.carto.ldse.interfaces;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.postgis.PGgeometry;

import uoa.carto.ldse.dc.QuadtreeBuilder;
import uoa.carto.ldse.dc.SummariesBuilder;
import uoa.carto.ldse.dm.desc.candidateDataset;

/**
 * 
 */

/**
 * @author vkopsachilis
 *
 */

// Class with methods for interacting with the System Database
public class DBInterface {
	
	// Credentials
	static String dburl = "jdbc:postgresql://localhost/okeanos";
	static String  user = "postgres";
	static String password = "postgres";
	
	
	public static void initDBforSummariesBuilder() throws SQLException{	
		emptyTempCoordsTable();
	}
	
	public static void finalizeDatasetCollector() throws SQLException{	
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			 con = DriverManager.getConnection(dburl, user, password);
			 //Delete inappropriate dataset ontologies
			 st = con.prepareStatement("Delete FROM in_dataset_ontology where number_of_features<5 or number_of_features>100000 or number_of_features is null or number_of_features=10000");
			 st.executeUpdate();
			
			 //Delete datasets that have not entry in dataset_ontology table
			 st = con.prepareStatement("Delete FROM in_datasets as d where d.id Not In (Select distinct dataset_id from in_dataset_ontology)");
			 st.executeUpdate();
			
				 
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 st.close();
	     con.close(); 	
	}
	
	public static void initDBforQuadtreeBuilder() throws SQLException{	
		emptyTempCoordsTable();
		createInitQuadtree();
	}
	
	public static void createInitQuadtree() throws SQLException{	
		Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			//Create Temp Quadtree
			//st = con.prepareStatement("drop table quadtree_global_temp");
			//st.executeUpdate();
			
			st = con.prepareStatement("Select * Into quadtree_global_temp From quadtree_global_template");
			st.executeUpdate();
			
			st = con.prepareStatement("ALTER TABLE quadtree_global_temp Drop COLUMN gid");
			st.executeUpdate();
			
			st = con.prepareStatement("ALTER TABLE quadtree_global_temp ADD COLUMN gid bigserial");
			st.executeUpdate();
			
			st = con.prepareStatement("CREATE INDEX quad_synch_idx ON quadtree_global_temp (geom ASC NULLS LAST)");
			st.executeUpdate();	
			
			//Initialize Final Quadtree Index
			//st = con.prepareStatement("drop table " + QuadtreeBuilder.indexTable);
			//st.executeUpdate();
			
			st = con.prepareStatement("Select * Into " + QuadtreeBuilder.indexTable + " From quadtree_05_global_template");
			st.executeUpdate();
			
			st = con.prepareStatement("ALTER TABLE " + QuadtreeBuilder.indexTable + " Drop COLUMN gid");
			st.executeUpdate();
			
			st = con.prepareStatement("ALTER TABLE " + QuadtreeBuilder.indexTable + " ADD COLUMN gid bigserial");
			st.executeUpdate();
			
			
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      
	}
		
	//Read CKAN Catalogs From Database
    public static List<String> getCKANCatalogs() throws SQLException{
    	List<String> catalogs=new ArrayList<String>();
    	Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select catalog From in_catalogs order by id");
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			while (rs.next()){
				catalogs.add(rs.getString(1));
			}
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return catalogs;
    } //End getCKANCatalogs 
    
    
    // Gets a Data Source id from the DB by its title. If the data source is not exist it return id=0
	public static int getDataSource(String title) throws SQLException{
		 int id = 0;
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("Select id from in_datasources Where datasource='"+title+"'");
			 st.executeQuery();
			 java.sql.ResultSet rs =  st.getResultSet();
			 if(st != null && rs.next()) id= rs.getInt(1);
		     
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 st.close();
	     con.close(); 
		 return id;
	} // End getDataSource
	
    
    // Adds a Data Source record in the DB. It returns the newly generated key
	public static int addDataSource(String title,int catalog) throws SQLException{
		 int id = 0;
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("INSERT INTO in_datasources(datasource, catalog_id) VALUES('"+title+"', "+catalog+")",Statement.RETURN_GENERATED_KEYS);
			 st.executeUpdate();
			 java.sql.ResultSet rs =  st.getGeneratedKeys();
			 if(st != null && rs.next()) id= rs.getInt(1);	     
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 st.close();
	     con.close(); 
		 return id;
	} // End addDataSource
	
	
	// Gets a Resource id from the DB by its url. If the resource is not exist it return id=0
	public static int getResourceID(String url) throws SQLException{
		 int id = 0;
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("Select id from in_datasource_resource Where url='"+url+"'");
			 st.executeQuery();
			 java.sql.ResultSet rs =  st.getResultSet();
			 if(st != null && rs.next()) id= rs.getInt(1);
		     
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 st.close();
	     con.close(); 
		 return id;
	} // End getResourceID
	
	 // Gets a Resource Url from the DB by its id. 
 	public static String getResourceUrlById(int id) throws SQLException{
 		 String url=null ;
 		 Connection con=null;
 		 PreparedStatement st=null;
 		 try {
 			
 			 con = DriverManager.getConnection(dburl, user, password);
 			 st = con.prepareStatement("Select url from in_datasource_resource Where id='"+id+"'");
 			 st.executeQuery();
 			 java.sql.ResultSet rs =  st.getResultSet();
 			 if(st != null && rs.next()) url = rs.getString(1);
 		     
 		  } catch (SQLException ex) {
 	          System.out.println(ex);
 		  }
 		 st.close();
 	     con.close(); 
 		 return url;
 	} // End getResourceUrlById
 	
	
	// Gets a Resource type from the DB by its url. If the resource is not exist it return type_id=0
		public static int getResourceTypeByURL(String url) throws SQLException{
			 int type_id = 0;
			 Connection con=null;
			 PreparedStatement st=null;
			 try {
				
				 con = DriverManager.getConnection(dburl, user, password);
				 st = con.prepareStatement("Select format_id from in_datasource_resource Where url='"+url+"'");
				 st.executeQuery();
				 java.sql.ResultSet rs =  st.getResultSet();
				 if(st != null && rs.next()) type_id= rs.getInt(1);
			     
			  } catch (SQLException ex) {
		          System.out.println(ex);
			  }
			 st.close();
		     con.close(); 
			 return type_id;
		} // End getResourceTypeByURL
		
		
	//Read Spatial Resources From Database
    public static List<String> getSpatialResources() throws SQLException{
    	List<String> spatialResources=new ArrayList<String>();
    	Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select url From in_datasource_resource where status='spatial' order by id");
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			while (rs.next()){
				spatialResources.add(rs.getString(1));
			}
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return spatialResources;
    } //End getSpatialResources 
	
	
	// Add a resource in the DB
	public static void addResource(int datasource,int format, String url,String status) throws SQLException{
		
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("INSERT INTO in_datasource_resource(datasource_id, format_id,url,status) VALUES("+datasource+","+format+" ,'"+url+"','"+status+"')");
			 st.executeUpdate();
			
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
	       st.close();
	       con.close();    		
	} // End addResource
	
	
	// Update a resource status in the DB
	public static void updateResource(int resource_id, String status) throws SQLException{		
		 Connection con=null;
		 PreparedStatement st=null;
		 try {
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("Update in_datasource_resource set status='"+status+"' Where id='"+resource_id+"'");
			 st.executeUpdate();	
		  } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 st.close();
	     con.close(); 		
	} // End updateResource
	
	// Gets a Dataset from the DB by its name. If the dataset is not exist it return id=0
	public static int getDatasetId(String dataset,int resource_id) throws SQLException{
		int id = 0;
		Connection con=null;
		PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select id from in_datasets Where dataset='"+dataset+"' and resource_id="+resource_id);
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			if(st != null && rs.next()) id= rs.getInt(1);
				     
		} catch (SQLException ex) {
			System.out.println(ex);
		}
		st.close();
		con.close(); 
		
		return id;
	} // End getDataset
		
 	//Read RResources Ids from datasets table 
    public static List<String> getResourceDatasets(int id) throws SQLException{
    	List<String> datasets=new ArrayList<String>();
    	Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select dataset From in_datasets where resource_id="+id+ " Order by id");
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			while (rs.next()){
				datasets.add(rs.getString(1));
			}
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return datasets;
    } //End getDatasetResource
    
    //Read Resources Ids from datasets table 
    public static List<Integer> getResourceDatasetsIds(int id) throws SQLException{
    	List<Integer> datasetsIds=new ArrayList<Integer>();
    	Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select id From in_datasets where resource_id="+id+ " Order by id");
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			while (rs.next()){
				datasetsIds.add(rs.getInt(1));
			}
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return datasetsIds;
    } //End getDatasetResourceIds
    
  //Read Resources Ids from datasets table 
    public static List<Integer> getDatasetResourceIds() throws SQLException{
    	List<Integer> resources=new ArrayList<Integer>();
    	Connection con=null;
	    PreparedStatement st=null;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select Distinct resource_id From in_datasets order by resource_id");
			st.executeQuery();
			java.sql.ResultSet rs =  st.getResultSet();
			while (rs.next()){
				resources.add(rs.getInt(1));
			}
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return resources;
    } //End getDatasetResourceIds
    
   
    	
	 // Adds a Dataset record in the DB. It returns the newly generated key
		public static int addDataset(String dataset,int resource_id) throws SQLException{
			 int id = 0;
			 Connection con=null;
			 PreparedStatement st=null;
			 try {
				 con = DriverManager.getConnection(dburl, user, password);
				 st = con.prepareStatement("INSERT INTO in_datasets(dataset, resource_id) VALUES('"+dataset+"', "+resource_id+")",Statement.RETURN_GENERATED_KEYS);
				 st.executeUpdate();
				 java.sql.ResultSet rs =  st.getGeneratedKeys();
				 if(st != null && rs.next()) id= rs.getInt(1);	     
			  } catch (SQLException ex) {
		          System.out.println(ex);
			  }
			 st.close();
		     con.close(); 
			 return id;
		} // End addDataset
	
	// Gets a Dataset Ontology from the DB by its name. If the dataset is not exist it return id=0
		public static int getDatasetOntologyId(int dataset_id,int ontology_id) throws SQLException{
			int id = 0;
			Connection con=null;
			PreparedStatement st=null;
			try {
				con = DriverManager.getConnection(dburl, user, password);
				st = con.prepareStatement("Select id from in_dataset_ontology Where dataset_id='"+dataset_id+"' and ontology_id="+ontology_id);
				st.executeQuery();
				java.sql.ResultSet rs =  st.getResultSet();
				if(st != null && rs.next()) id= rs.getInt(1);
					     
			} catch (SQLException ex) {
				System.out.println(ex);
			}
			st.close();
			con.close(); 
			
			return id;
		} // End getDatasetOntology
		
		 // Gets a Dataset Ontology from the DB by its name. If the dataset is not exist it return id=0
 		public static List<Integer> getDatasetOntologies(int dataset_id) throws SQLException{
 			List<Integer> datasetOntologiesIds=new ArrayList<Integer>();
 			Connection con=null;
 			PreparedStatement st=null;
 			try {
 				con = DriverManager.getConnection(dburl, user, password);
 				st = con.prepareStatement("Select ontology_id from in_dataset_ontology Where dataset_id="+dataset_id);
 				st.executeQuery();
 				java.sql.ResultSet rs =  st.getResultSet();
 				while (rs.next()){
 					datasetOntologiesIds.add(rs.getInt(1));
 				}
 					     
 			} catch (SQLException ex) {
 				System.out.println(ex);
 			}
 			st.close();
 			con.close(); 
 			
 			return datasetOntologiesIds;
 		} // End getDatasetOntology

		
		 // Adds a Dataset Ontology record in the DB. It returns the newly generated key
		public static int addDatasetOntology(int dataset_id,int ontology_id, int number_of_features) throws SQLException{
			 int id=0;
			 Connection con=null;
			 PreparedStatement st=null;
			 try {
				 con = DriverManager.getConnection(dburl, user, password);
				 st = con.prepareStatement("INSERT INTO in_dataset_ontology(dataset_id, ontology_id,number_of_features) VALUES('"+dataset_id+"', "+ontology_id+","+number_of_features+")",Statement.RETURN_GENERATED_KEYS);
				 st.executeUpdate();
				 java.sql.ResultSet rs =  st.getGeneratedKeys();
				 if(st != null && rs.next()) id= rs.getInt("id");	     
				   
			  } catch (SQLException ex) {
		          System.out.println(ex);
			  }
			 st.close();
		     con.close(); 
		     
			 return id;
		} // End addDatasetOntology
	
		
		
	// Update a Data Source and resource in DB
	public static void updateDatasourcesInDB(int catalogID, String datasource, String resource_url, int resource_type, String status) throws SQLException{	
		 // Get a resource's id. if it does not exist return id=0 
		 int resource_id= getResourceID(resource_url);
		
		 // if the resource does not exist add it
		 if (resource_id==0){
			 int datasource_id= getDataSource(datasource);
			 // if the data source also does not exist, also add it
			 if (datasource_id==0){
				 int new_datasource_id=addDataSource(datasource,catalogID); 
				 addResource(new_datasource_id,resource_type,resource_url,status);
			 } else {
				 addResource(datasource_id,resource_type,resource_url,status);
			 } 	
		 // if the resource exist update only its status	 
		 } else {
			 updateResource(resource_id,status); 
		 }
			
	 }// End updateDatasourcesInDB
	
	public static int updateDatasetsinDB(String dataset, int resource_id, int ontology_id, int number_of_features) throws SQLException{
		System.out.println("Adding Dataset in DB...");
		System.out.println("Dataset:"+dataset);
		System.out.println("Resource:"+resource_id);
		System.out.println("Ontology:"+ontology_id);
		int dataset_ontology_id=0;
		int dataset_id=getDatasetId(dataset,resource_id);
		System.out.println("DatasetID:"+dataset_id);
		if (dataset_id>0){
			dataset_ontology_id=getDatasetOntologyId(dataset_id,ontology_id);
			if (dataset_ontology_id==0){
				dataset_ontology_id=addDatasetOntology(dataset_id,ontology_id,number_of_features);
			}
		}else{
			int new_dataset_id=addDataset(dataset,resource_id);
			dataset_ontology_id=addDatasetOntology(new_dataset_id,ontology_id,number_of_features);
		}
		System.out.println("DatasetOntologyId:"+dataset_ontology_id);
		return dataset_ontology_id;
	}
	
	
	public static void buildQuadtreeSummary( int number_of_features, int dataset_ontology_id) throws SQLException{
		 Connection con=null;
		 PreparedStatement st=null;
		 String query;
		 try {
			 System.out.println("Building Summary...");
			 con = DriverManager.getConnection(dburl, user, password);
			 st = con.prepareStatement("Update in_dataset_ontology set bbox=(Select ST_Extent(geom) From tempcoords), convexhull=(Select ST_convexHull(St_Collect(geom)) From tempcoords), number_of_features="+number_of_features+" Where id="+dataset_ontology_id+"");
			 st.executeUpdate();
			 
			 query= "Delete from in_quadtree_summaries where dataset_ontology_id="+dataset_ontology_id;
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
			 query =	"Insert Into in_quadtree_summaries(dataset_ontology_id,cell_id)"+
						" Select Distinct "+dataset_ontology_id+",g.gid "+
						" from " + SummariesBuilder.indexTable+ " as g, tempcoords as t " +
						" where ST_Intersects(g.geom, (Select ST_Extent(geom) From tempcoords)) and ST_Intersects(g.geom,t.geom) ";
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 		
		 
		 } catch (SQLException ex) {
	          System.out.println(ex);
		  }
		 
		 st.close();
	     con.close(); 		
	}
		
  
	public static void updateDatasetsInQuadtree() throws SQLException{	
		Connection con=null;
		PreparedStatement st=null;
		String query;
		System.out.println("Updating Datasets in Quadtree");
		try {
			con = DriverManager.getConnection(dburl, user, password);
			query="UPDATE quadtree_global_temp as g1 SET datasets=datasets+1 " +
				 "Where g1.gid IN (Select Distinct g1.gid From tempcoords as t1 Where ST_Intersects(g1.geom,t1.geom)) ";          
			st = con.prepareStatement(query);
			st.executeUpdate();
		 
		} catch (SQLException ex) {
          System.out.println(ex);
		}
	 
		st.close();
		con.close(); 
	}

	public static void prepareNextQuadtreeScale() throws SQLException{
		 Connection con=null;
		 PreparedStatement st=null;
		 String query;
		 try {
			 con = DriverManager.getConnection(dburl, user, password);
			 query="Insert into " + QuadtreeBuilder.indexTable + " (geom) Select geom From quadtree_global_temp where datasets<"+QuadtreeBuilder.divisionCriterion;
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
			 query="Delete FROM quadtree_global_temp where datasets<"+QuadtreeBuilder.divisionCriterion;
			 
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
			 query="Insert Into quadtree_global_temp (geom,datasets) Select  (ST_Dump(ST_Split(geom2, ST_MakeLine(ST_MakePoint((ST_Xmin(geom2)+ST_Xmax(geom2))/2,ST_Ymin(geom2)),ST_MakePoint((ST_Xmin(geom2)+ST_Xmax(geom2))/2, ST_Ymax(geom2)))))).geom, 0 "+
			 " from (SELECT (ST_Dump(ST_Split(geom, ST_MakeLine(ST_MakePoint(ST_Xmin(geom), (ST_Ymin(geom)+ST_Ymax(geom))/2),ST_MakePoint(ST_Xmax(geom), (ST_Ymin(geom)+ST_Ymax(geom))/2))))).geom as geom2 "+
			 " FROM quadtree_global_temp ) g2";
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
			 query="Delete FROM quadtree_global_temp where datasets>="+QuadtreeBuilder.divisionCriterion;
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
			 query = "REINDEX INDEX quad_synch_idx";
			 st = con.prepareStatement(query);
			 st.executeUpdate();
			 
		 } catch (SQLException ex) {
	          System.out.println(ex);
		 }
		 
		st.close();
	    con.close(); 		
	}
	
	  public static void AddGeometriesToTempCoordsTable(List<String> classGeometries) throws SQLException{
	    	 Connection con=null;
			 PreparedStatement st=null;
			 String query;
			 try {
				 con = DriverManager.getConnection(dburl, user, password);
				 System.out.println("Adding " + classGeometries.size()+ " to TempCoords");
				 for (int c=1; c<=classGeometries.size();c++){	
					 if (classGeometries.get(c-1).contains(",")){
						 try{
						 String[] xy=classGeometries.get(c-1).split(",");
						 if (c==1) System.out.println("X:"+ Double.parseDouble(xy[0]) +" Y:"+Double.parseDouble(xy[1]) );
						 query ="INSERT INTO tempcoords(geom) VALUES(ST_GeomFromText('POINT("+Double.parseDouble(xy[0])+" "+Double.parseDouble(xy[1])+")', 0))";
						 st = con.prepareStatement(query);
						 st.executeUpdate();
						  }catch (NumberFormatException ex) {
					          System.out.println("NumberFormatException: "+ex);
						  }
					 }
				 }
				 
				 query = "REINDEX INDEX jintert";
				 st = con.prepareStatement(query);
				 st.executeUpdate();
				 
			 } catch (SQLException ex) {
		          System.out.println(ex);
			  }
			 
			st.close();
		    con.close();     	
	    }
	    
	    public static void emptyTempCoordsTable() throws SQLException{
	   	 Connection con=null;
			 PreparedStatement st=null;
			 String query;
			 try {
				 con = DriverManager.getConnection(dburl, user, password);
				 query =	"Delete From tempCoords"; 
				 st = con.prepareStatement(query);
				 st.executeUpdate();
				 
				 query = "REINDEX INDEX jintert";
				 st = con.prepareStatement(query);
				 st.executeUpdate();
				 
			 } catch (SQLException ex) {
		          System.out.println(ex);
			  }	 
			st.close();
		    con.close();   	
	   }
	    
	    public static void addHierarchy(int child, int parent) throws SQLException{
		   	     Connection con=null;
				 PreparedStatement st=null;
				 String query;
				 try {
					 con = DriverManager.getConnection(dburl, user, password);
					 query ="INSERT INTO in_dataset_hierarchy(child, parent) VALUES("+child+","+parent+")"; 
					 st = con.prepareStatement(query);
					 st.executeUpdate();
					 
				 } catch (SQLException ex) {
			          System.out.println(ex);
				  }	 
				st.close();
			    con.close();   	
		   }  
	    
	   /////////////////////////////////////////////////////////////////
	    //////////////////////////////////////////////////////////////
	    ///////////////////////////////////////////////////////////
	    
	    public static List<Integer> getSampleDatasets(String table) throws SQLException{
	    	List<Integer> datasets=new ArrayList<Integer>();
	    	Connection con=null;
		    PreparedStatement st=null;
			try {
				con = DriverManager.getConnection(dburl, user, password);
				st = con.prepareStatement("Select id From " + table );
				st.executeQuery();
				java.sql.ResultSet rs =  st.getResultSet();
				while (rs.next()){
					datasets.add(rs.getInt(1));
				}
			} catch (SQLException ex) {
				System.out.println(ex);
			}
		    st.close();
		    con.close();      	
	    	return datasets;
	    } //End getDatasetResource
	    
	    public static void buildIntersectionTableforDataset(int dataset_id, String summariesTable,String quadtreeTable, String inputDatasetsTable, String testDatasetsTable) throws SQLException{
	    	Connection con=null;
		    PreparedStatement st=null;
		    PreparedStatement st2=null;
		   
		    try {
			
		    	con = DriverManager.getConnection(dburl, user, password);
				st = con.prepareStatement("INSERT INTO intersections(dataset_ontology_id,commoncells) Select dataset_ontology_id, count(dataset_ontology_id) From " + summariesTable + " Where cell_id IN (Select cell_id From " + summariesTable + " Where dataset_ontology_id="+dataset_id+") and dataset_ontology_id IN (Select id from " + testDatasetsTable + " where id!="+dataset_id+") group by dataset_ontology_id having count(dataset_ontology_id)>2");
				st.executeUpdate();
				
				 st = con.prepareStatement("UPDATE intersections as i SET geom = St_intersection(d1.convexhull, d2.convexhull) from " + testDatasetsTable + " as d1, " + inputDatasetsTable+ " as d2 Where i.dataset_ontology_id=d1.id and d2.id="+dataset_id);
				 st.executeUpdate();
				 
				 st = con.prepareStatement("Reindex index jbbox2");
				 st.executeUpdate();
				 
				 
				 st = con.prepareStatement("Select count(dataset_ontology_id) From intersections");
				 st.executeQuery();	
				 java.sql.ResultSet rs = st.getResultSet();
				 int count=0;
				 while (rs.next()){
					 count=rs.getInt(1);
				 }
				 
				 if (count>0){
				 st = con.prepareStatement("Select dataset_ontology_id, geom From intersections");
				 st.executeQuery();
				 int test_dataset_id=0;
				 PGgeometry intersection;
				 rs = st.getResultSet();
			    	while (rs.next()){
				   
					 test_dataset_id = rs.getInt(1);
					 intersection = (PGgeometry) rs.getObject(2);
								 
					 st2 = con.prepareStatement("UPDATE intersections as i SET datasetbcells = (Select count(gr1.gid) from  "+quadtreeTable+" as gr1 INNER JOIN (Select cell_id From "+summariesTable+"  where  dataset_ontology_id="+test_dataset_id+") as g1 ON gr1.gid=g1.cell_id Where ST_intersects(gr1.geom,ST_GeomFromText('"+intersection+"'))) where i.dataset_ontology_id="+test_dataset_id);
					 st2.executeUpdate();
				
					 st2 = con.prepareStatement("UPDATE intersections as i SET datasetacells = (Select count(gr1.gid) from "+quadtreeTable+" as gr1 INNER JOIN (Select cell_id From "+summariesTable+"  where dataset_ontology_id="+dataset_id+" ) as g1 ON gr1.gid=g1.cell_id Where ST_intersects(gr1.geom,ST_GeomFromText('"+intersection+"'))) where i.dataset_ontology_id="+test_dataset_id);
					 st2.executeUpdate();	
				
					 st2 = con.prepareStatement( "UPDATE intersections as i SET extent = (Select count(gr1.gid) from  "+quadtreeTable+" as gr1 Where ST_intersects(gr1.geom,ST_GeomFromText('"+intersection+"'))) where i.dataset_ontology_id="+test_dataset_id);
					 st2.executeUpdate();
					 
					 st2.close(); 
				 }
			    }
		    } catch (SQLException ex) {
				System.out.println(ex);
			}
			
		    st.close();
		   
		    con.close();      
	    } // End buildIntersectionsTableforDatasets
	    
	    public static void initDBforDatasetMatcher(String resultsTable) throws SQLException{
	    	emptyIntersectionsTable();
	    //	emptyMatcherResultsTable(resultsTable);
	    	
	    }

	    public static void emptyIntersectionsTable() throws SQLException{
		   	 	Connection con=null;
				PreparedStatement st=null;
				
				 try {
					 
					 con = DriverManager.getConnection(dburl, user, password);				
					 st = con.prepareStatement("Delete From intersections");
					 st.executeUpdate();
		 
				 } catch (SQLException ex) {
			          System.out.println(ex);
				  }	 
				st.close();
			    con.close();   	
		   } 
	    
	    public static void emptyMatcherResultsTable(String resultsTable) throws SQLException{
	   	 	Connection con=null;
			PreparedStatement st=null;
			
			 try {
				 
				 con = DriverManager.getConnection(dburl, user, password);				
				 st = con.prepareStatement("Delete From "+ resultsTable);
				 st.executeUpdate();
	 
			 } catch (SQLException ex) {
		          System.out.println(ex);
			  }	 
			st.close();
		    con.close();   	
	   } 
	    
	   public static List<candidateDataset> getCandidateDatasets() throws SQLException{
		   List<candidateDataset> datasets=new ArrayList<candidateDataset>();
	    	Connection con=null;
		    PreparedStatement st=null;
			try {
				con = DriverManager.getConnection(dburl, user, password);
				st = con.prepareStatement("Select dataset_ontology_id, extent, datasetacells, datasetbcells, commoncells From intersections" );
				st.executeQuery();
				java.sql.ResultSet rs =  st.getResultSet();
				while (rs.next()){
					datasets.add(new candidateDataset(rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getInt(4),rs.getInt(5)));
				}
			} catch (SQLException ex) {
				System.out.println(ex);
			}
		    st.close();
		    con.close();      	
	    	return datasets;
	   }
	   
	   public static void addResultsToDB(String resultsTable, int datAId, String datAName, int datBId,String datBName, int extent, int datACells,int datBCells, int commoncells, double jaccard, double overlap, double hg, double ir, double x2, double fisher ) throws SQLException{
	   	     Connection con=null;
			 PreparedStatement st=null;
			 String query;
			 try {
				 con = DriverManager.getConnection(dburl, user, password);
				 query = "INSERT INTO "+resultsTable+" (datasetaid,datasetaname,datasetbid,datasetbname,extent,datasetacells,datasetbcells,"+
						 "commoncells,jaccard,subset,hypergeometry,indratio,xsquare,fisher) VALUES "+
					     "("+datAId+",'"+datAName+"',"+datBId+",'"+datBName+"',"+extent+","+datACells+","+datBCells+","+commoncells+","+
					      jaccard+","+overlap+","+ hg+","+ir+","+x2+","+fisher+")"; 
				 st = con.prepareStatement(query);
				 st.executeUpdate();
				 
			 } catch (SQLException ex) {
		          System.out.println(ex);
			  }	 
			st.close();
		    con.close();   	
	   }  
	   
	// Gets a Dataset from the DB by its name. If the dataset is not exist it return id=0
		public static String getDatasetNameById(int dataset_ontology_id) throws SQLException{
			String dataset="" ;
			Connection con=null;
			PreparedStatement st=null;
			try {
				con = DriverManager.getConnection(dburl, user, password);
				st = con.prepareStatement("Select dataset from in_datasets ,in_dataset_ontology Where  in_datasets.id=in_dataset_ontology.dataset_id and in_dataset_ontology.id="+dataset_ontology_id);
				st.executeQuery();
				java.sql.ResultSet rs =  st.getResultSet();
				if(st != null && rs.next()) dataset= rs.getString(1);
					     
			} catch (SQLException ex) {
				System.out.println(ex);
			}
			st.close();
			con.close(); 
			
			return dataset;
		} // End getDataset
	    
		
	public static boolean isDatasetPairAlreadyInDB(String resultsTable, int data, int datb) throws SQLException{
		Connection con=null;
	    PreparedStatement st=null;
	    boolean exist=false;
		try {
			con = DriverManager.getConnection(dburl, user, password);
			st = con.prepareStatement("Select count(datasetaid) From "+ resultsTable + " where datasetaid="+data+" and datasetbid="+datb);
			st.executeQuery();	
			java.sql.ResultSet rs = st.getResultSet();
			while (rs.next()){
				if (rs.getInt(1)>0) exist=true;
			}
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	    st.close();
	    con.close();      	
    	return exist;
		
	}
} // End Class

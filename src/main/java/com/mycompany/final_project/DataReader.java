package com.mycompany.final_project;
import java.awt.Color;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.spark.sql.Encoders;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;



public class DataReader {
    
        public static void main(String[] args) throws IOException {
            
            // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("AirBnb Analysis Demo").master ("local[6]")
                .getOrCreate ();
        
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        
        
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
         Dataset<Row> airbnbDF = dataFrameReader.csv ("src/main/resourses/Wuzzuf_Jobs.csv");

         
        airbnbDF= airbnbDF.select (  "Title" , "Company" , "Location" , "Type" , "Level" , "YearsExp" , "Country","Skills");
        //final Dataset<Row> airbnbNoNullDF=airbnbDF.na().drop ();
       // airbnbDF.printSchema ();
        System.out.println (airbnbDF.count ());
 


    //display structure of the dataframe
airbnbDF.printSchema ();

   
    //cleaning data
airbnbDF.dropDuplicates().show();





 //get number of jobs for each company

airbnbDF.createOrReplaceTempView ("jobs_for_each_company");
Dataset<Row> jobs_company = sparkSession.sql ("select Company,count(distinct(Title)) as count from jobs_for_each_company  GROUP BY Company order by count DESC ");
jobs_company.show();

     //convert dataset to list 
Dataset<Row> cocount = jobs_company.select("count");
List<Float> yData  = cocount.as(Encoders.FLOAT()).collectAsList();
List<Float> result = new ArrayList<Float>();
int listSize = yData.size();
for (int i = 0; i < listSize; ++i) {
  
     // accessing each element of array
result.add(yData.get(i));
        };

//Collections.sort(result, Collections.reverseOrder());

     //jobs_company.show();
Dataset<Row> conames = jobs_company.select("Company");
List<String> xData  = conames.as(Encoders.STRING()).collectAsList();
List<String> xresult = new ArrayList<String>();
for (int i = 0; i < xData.size(); ++i) {
  
            // accessing each element of array
            xresult.add(xData.get(i));
        };
/*for (int i = 0; i < result.size(); ++i) {
            // accessing each element of array
            System.out.println(result.get(i));
            System.out.println(xresult.get(i));
        };*/
//System.out.println(xData.getClass().getSimpleName()); 
//jobs_company.collectAsList().stream().forEach(System.out::println);
//String[] xData =  jobs_company[0];
//Float[] yData = jobs_company[1];
// Create Chart
//XYChart chart = QuickChart.getChart("Sample Chart", "X", "Y", "y(x)", xData, yData);

// Create Chart
PieChart chart = new PieChartBuilder().width(600).height(500).title("jobs for first 5 companys").build();
// Customize Chart
//Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
//chart.getStyler ().setSeriesColors (sliceColors);
// Series
chart.addSeries (xresult.get(0), result.get(0));
chart.addSeries (xresult.get(1), result.get(1));
chart.addSeries (xresult.get(2), result.get(2));
chart.addSeries (xresult.get(3), result.get(3));
chart.addSeries (xresult.get(4), result.get(4));

// Show it
new SwingWrapper(chart).displayChart();

//Save it
BitmapEncoder.saveBitmap(chart, "./Sample_Chart", BitmapFormat.PNG);

/*DataReader xChartExamples = new DataReader ();
//List<Float> jobs_company = new List<Float>;

xChartExamples.graphPassengerClass(jobs_company);*/


//get most jobs_counting
airbnbDF.createOrReplaceTempView ("jobs_counting");
Dataset<Row> jobs_counting = sparkSession.sql ("select Title,count(Title) as mostCommon_job from jobs_counting GROUP BY Title  order by mostCommon_job DESC ");
jobs_counting.show();
Dataset<Row> tcount = jobs_counting.select("mostCommon_job");
List<Float> t_count  = tcount.as(Encoders.FLOAT()).collectAsList();
List<Float> t_count_list = new ArrayList<Float>();
for (int i = 0; i < 10; ++i) {
  
            // accessing each element of array
            t_count_list.add(t_count.get(i));
        };
//Collections.sort(result, Collections.reverseOrder());

//jobs_company.show();
Dataset<Row> title = jobs_counting.select("Title");
List<String> title_x  = title.as(Encoders.STRING()).collectAsList();
List<String> title_list = new ArrayList<String>();
for (int i = 0; i < 10; ++i) {
            // accessing each element of array
            title_list.add(title_x.get(i));};
            
/*for (int i = 0; i < result.size(); ++i) {
            // accessing each element of array
            System.out.println(title_list.get(i));
            System.out.println(t_count_list.get(i));};*/

CategoryChart barchart = new CategoryChartBuilder ().width (1024).height (768).title ("Most popular job titles Histogram").xAxisTitle ("Names").yAxisTitle ("Count").build ();
// Customize Chart
barchart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
barchart.getStyler ().setHasAnnotations (true);
barchart.getStyler ().setStacked (true);
// Series
barchart.addSeries ("Title's Counts", title_list, t_count_list);
// Show it
new SwingWrapper (barchart).displayChart ();

BitmapEncoder.saveBitmap(barchart, "./bar1_Chart", BitmapFormat.PNG);





 //get most areas_counting
airbnbDF.createOrReplaceTempView ("areas_counting");
Dataset<Row> areas_counting =  sparkSession.sql ("select Location,count(Location) as mostCommon_area from areas_counting GROUP BY Location  order by mostCommon_area DESC ");
areas_counting.show();
 
Dataset<Row> area_count = areas_counting.select("mostCommon_area");
List<Float> a_count  = area_count.as(Encoders.FLOAT()).collectAsList();
List<Float> a_count_list = new ArrayList<Float>();
for (int i = 0; i < 10; ++i) {
  
            // accessing each element of array
            a_count_list.add(a_count.get(i));
        };
//Collections.sort(result, Collections.reverseOrder());

//jobs_company.show();
Dataset<Row> location = areas_counting.select("Location");
List<String> location_x  = location.as(Encoders.STRING()).collectAsList();
List<String> location_list = new ArrayList<String>();
for (int i = 0; i < 10; ++i) {
            // accessing each element of array
            location_list.add(location_x.get(i));};
            
/*for (int i = 0; i < result.size(); ++i) {
            // accessing each element of array
            System.out.println(title_list.get(i));
            System.out.println(t_count_list.get(i));};*/

CategoryChart barchart2 = new CategoryChartBuilder ().width (1024).height (768).title ("Most Popular Areas Histogram").xAxisTitle ("Area").yAxisTitle ("Count").build ();
// Customize Chart
barchart2.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
barchart2.getStyler ().setHasAnnotations (true);
barchart2.getStyler ().setStacked (true);
// Series
barchart2.addSeries ("Area's Counts", location_list, a_count_list);
// Show it
new SwingWrapper (barchart2).displayChart (); 
 
 BitmapEncoder.saveBitmap(barchart2, "./bar2_Chart", BitmapFormat.PNG);

 
 
 
 
 
 

 //Factorize YearExp
airbnbDF.createOrReplaceTempView ("start_of_exp");
 sparkSession.sql("select * FROM start_of_exp WHERE YearsExp not LIKE '%null Yrs of Exp%'");
 sparkSession.sql ("SELECT (SUBSTRING(YearsExp, 1, 1)) As start from (select * FROM start_of_exp WHERE YearsExp not LIKE '%null Yrs of Exp%')").show();

 //Factorize YearExp
airbnbDF.createOrReplaceTempView ("last_of_exp");
 sparkSession.sql("select * FROM last_of_exp WHERE YearsExp not LIKE '%null Yrs of Exp%'");
 sparkSession.sql ("SELECT ( SUBSTRING(YearsExp, 3, 2)) As last from (select * FROM last_of_exp WHERE (YearsExp not LIKE '%null Yrs of Exp%') AND (YearsExp not LIKE '[a-zA-Z][a-zA-Z]%')      ) ").show();


 //AND  ISNUMERIC( SUBSTRING(YearsExp, 3, 2)=1)
 





airbnbDF.createOrReplaceTempView ("skills_counting");
//the following section breaks down the \"skills\" column into three columns.
//Dataset<Row> skills_counting =  sparkSession.sql ("select part1 from(SELECT split(Skills, ',')[0] as part1, split(Skills, ',')[1] as part2 , split(Skills, ',')[2] as part3 ,split(Skills, ',')[3] as part4 , split(Skills, ',')[4] as part5 from skills_counting) union select part1 from(SELECT split(Skills, ',')[0] as part1, split(Skills, ',')[1] as part2 , split(Skills, ',')[2] as part3 ,split(Skills, ',')[3] as part4 , split(Skills, ',')[4] as part5 from skills_counting)");
//skills_counting.show();

Dataset<Row> skills_counting =  sparkSession.sql ("select col , count(col) as No_of_skills  FROM (SELECT explode(split(Skills, ',')) as col from skills_counting)group by col order by No_of_skills  desc");
skills_counting.show();
    
        }
       
}

       
        
/////////////////////////////////////////////////////////////////////////////////////////////////

     // JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        //summary stat using spark
// $example on$
  // JavaRDD<Vector> mat = jsc.parallelize(
    //  Arrays.asList(
     //   Vectors.dense(1.0, 10.0, 100.0),
     //   Vectors.dense(2.0, 20.0, 200.0),
     //   Vectors.dense(3.0, 30.0, 300.0)
    //  )
   // ); // an RDD of Vectors

    
     //JavaRDD<String> data = jsc.textFile("src/main/resourses/Wuzzuf_Jobs.csv");
//JavaRDD<Vector> datamain = data.flatMap(null);
//MultivariateStatisticalSummary mat = Statistics.colStats(datamain.rdd());

    
    
    // Compute column summary statistics.
   // MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
   // System.out.println(summary.mean());  // a dense vector containing the mean value for each column
    //System.out.println(summary.variance());  // column-wise variance
   // System.out.println(summary.numNonzeros());  // number of nonzeros in each column
    // $example off$

   // jsc.stop();
   ////////////////////////////////////////////////////////////////////////////////////////////////
    
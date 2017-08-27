import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;


public class Driver {

	public static void main(String[] args) {
		
			    	
		    		//configure the first phase
		    		
					try {
						Configuration conf1 = new Configuration();
				    	Job job1;
						job1 = new Job(conf1, "Phase 1");
						job1.setJarByClass(Phase1.class);
				        job1.setMapperClass(Phase1.Mapper1.class);
				        job1.setMapOutputKeyClass(Text.class);
				        job1.setMapOutputValueClass(Text.class);
				        job1.setReducerClass(Phase1.Reducer1.class);
				        job1.setOutputKeyClass(Text.class);
				        job1.setOutputValueClass(Text.class);
				        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:50071/dsp3input"));
				        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:50071/dsp3output1"));
				        //execute first phase
			            if (job1.waitForCompletion(true)){
			            	System.out.println("phase 1 finished");
			            	//get lexeme and feature total count
			            	Counter lexemeCounter = job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "LEXEME");
			            	Counter featureCounter = job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "FEATURE");
			            	System.out.println("total lexemes: " + lexemeCounter.getValue());
			            	System.out.println("total features: " + featureCounter.getValue());
			            	//merge all the output of phase 1 into a single file in the hdfs
			            	FileSystem fs = FileSystem.get(new URI("hdfs://localhost:50071"),conf1);
			                Path srcPath = new Path("hdfs://localhost:50071/dsp3output1");
			                Path dstPath = new Path("hdfs://localhost:50071/dsp3output/output1.txt");
			                if (FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf1, null)){
			                    System.out.println("files merged successfully");
			                }
			            }
			            
			            Configuration conf2 = new Configuration();
						conf2.setLong("TotalLexemes", job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "LEXEME").getValue());
						conf2.setLong("TotalFeatures", job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "FEATURE").getValue());
						Job job2;
						job2 = new Job(conf2, "Phase 2");
						job2.setJarByClass(Phase2.class);
				        job2.setMapperClass(Phase2.Mapper2.class);
				        job2.setMapOutputKeyClass(Text.class);
				        job2.setMapOutputValueClass(Text.class);
//				        job2.setReducerClass(Phase2.Reducer2.class);
				        job2.setNumReduceTasks(0);
				        job2.setOutputKeyClass(Text.class);
				        job2.setOutputValueClass(Text.class);
				        FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:50071/dsp3output1"));
				        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:50071/dsp3output2"));
				        //execute first phase
			            if (job2.waitForCompletion(true)){
			            	System.out.println("phase 2 finished");
			            	FileSystem fs = FileSystem.get(new URI("hdfs://localhost:50071"),conf2);
			                Path srcPath = new Path("hdfs://localhost:50071/dsp3output2");
			                Path dstPath = new Path("hdfs://localhost:50071/dsp3output/output2.txt");
			                if (FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf2, null)){
			                    System.out.println("files merged successfully");
			                }
			            }
			            
			            Configuration conf3 = new Configuration();
//						conf2.setLong("TotalLexemes", job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "LEXEME").getValue());
//						conf2.setLong("TotalFeatures", job1.getCounters().findCounter("Phase1$Reducer1$CountersEnum", "FEATURE").getValue());
						Job job3;
						job3 = new Job(conf3, "Phase 3");
						job3.setJarByClass(Phase3.class);
				        job3.setMapperClass(Phase3.Mapper3.class);
//				        job3.setMapOutputKeyClass(Text.class);
//				        job3.setMapOutputValueClass(Text.class);
				        job3.setReducerClass(Phase3.Reducer3.class);
				        job3.setOutputKeyClass(Text.class);
				        job3.setOutputValueClass(Text.class);
				        FileInputFormat.addInputPath(job3, new Path("hdfs://localhost:50071/dsp3output2"));
				        FileOutputFormat.setOutputPath(job3, new Path("hdfs://localhost:50071/dsp3output3"));
			            if (job3.waitForCompletion(true)){
			            	System.out.println("phase 3 finished");
//			            	FileSystem fs = FileSystem.get(new URI("hdfs://localhost:50071"),conf1);
//			                Path srcPath = new Path("hdfs://localhost:50071/dsp3output2");
//			                Path dstPath = new Path("hdfs://localhost:50071/dsp3output2/output2.txt");
//			                if (FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf1, null)){
//			                    System.out.println("files merged successfully");
//			                }
			            }
					} catch (IOException | ClassNotFoundException | InterruptedException | URISyntaxException e) {
						e.printStackTrace();
					}
					
					
					
					
			        
		
			} 
		
		}

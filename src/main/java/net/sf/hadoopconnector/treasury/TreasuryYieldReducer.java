package net.sf.hadoopconnector.treasury;




import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


/**
* The treasury yield reducer.
*/
public class TreasuryYieldReducer
     extends Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {
 @Override
 public void reduce( final IntWritable pKey,
                     final Iterable<DoubleWritable> pValues,
                     final Context pContext )
         throws IOException, InterruptedException{
     int count = 0;
     double sum = 0;
     for ( final DoubleWritable value : pValues ){
         sum += value.get();
         count++;
         LOG.info( "Key: " + pKey + " Value: " + value + " count: " + count + " sum: " + sum );
     }

     final double avg = sum / count;

     LOG.debug( "Average 10 Year Treasury for " + pKey.get() + " was " + avg );

     BasicBSONObject output = new BasicBSONObject();
     output.put("count", count);
     output.put("avg", avg);
     output.put("sum", sum);
     pContext.write( pKey, new BSONWritable( output ) );
 }

 private static final Log LOG = LogFactory.getLog( TreasuryYieldReducer.class );

}


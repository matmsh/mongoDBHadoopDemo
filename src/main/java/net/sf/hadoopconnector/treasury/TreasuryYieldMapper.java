package net.sf.hadoopconnector.treasury;


import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;


/**
* The treasury yield mapper.
*/
public class TreasuryYieldMapper
     extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {

 @Override
 public void map( final Object pKey,
                  final BSONObject pValue,
                  final Context pContext )
         throws IOException, InterruptedException{

     @SuppressWarnings("deprecation")
	final int year = ((Date)pValue.get("_id")).getYear() + 1900;
     double bid10Year = ( (Number) pValue.get( "bc10Year" ) ).doubleValue();

     pContext.write( new IntWritable( year ), new DoubleWritable( bid10Year ) );
 }

 @SuppressWarnings("unused")
private static final Log LOG = LogFactory.getLog( TreasuryYieldMapper.class );
}


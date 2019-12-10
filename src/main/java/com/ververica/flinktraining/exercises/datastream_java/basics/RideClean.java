package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideClean {
	public static void main(String[] args) throws Exception {
		ParameterTool arg = ParameterTool.fromArgs(args);
		String input = arg.get("inpur", ExerciseBase.pathToRideData);
		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
		DataStream<TaxiRide> taxiRide = env
			.addSource(ExerciseBase.rideSourceOrTest(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));
		DataStream<TaxiRide> taxiRideTrans = taxiRide.filter(
				ride -> GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat));
        // new NYCFilter());
		taxiRideTrans.print();
		env.execute("start in NYC");
	}
}
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import net.opentsdb.core.Aggregator.Doubles;
import net.opentsdb.core.Aggregator.Longs;
import net.opentsdb.core.Aggregators.Std;

import org.junit.Test;

public class TestStandardDeviationAggregator {

    /**
     * Instead of using a fixed epsilon to compare our numbers, I decided calculate
     * the epsilon based on the percentage of our actual expected values. E.g.
     * if our expected Standard Deviation is 1200000000 then the epsilon will be
     * and our epsilon percentage is 1/10000000's of a percent,
     * the actual epsilon will be 1200000000 * (1/(10000000 * 100)) = 1.2.
     * I decided to do it this way because our numbers are extremely large
     * and if you change the scale of the numbers a static precision may no longer work
     */
    public static final double EPSILON_PERCENTAGE = (00000000.1d / 100d);

    public static class StaticLongs implements Longs {

        public Iterator<Long> t;

        public StaticLongs(List<Long> samples) {
            t = samples.iterator();
        }

        @Override
        public boolean hasNextValue() {
            return t.hasNext();
        }

        @Override
        public long nextLongValue() {
            return t.next();
        }
    }

    public static class StaticDoubles implements Doubles {

        public Iterator<Double> t;

        public StaticDoubles(List<Double> samples) {
            t = samples.iterator();
        }

        @Override
        public boolean hasNextValue() {
            return t.hasNext();
        }

        @Override
        public double nextDoubleValue() {
            return t.next();
        }
    }

    @Test
    public void testLongAggregation() {
        Random random = new Random();

        List<Long> longsList = new ArrayList<Long>();
        List<Double> doublesList = new ArrayList<Double>();

        for(int i=0; i<1000; i++) {
            long longValue = Math.abs(random.nextLong() * 198764321257l) % Long.MAX_VALUE;
            //long longValue = Math.abs(random.nextInt(100)); //try this for smaller values
            double doubleValue = (double) longValue;

            doublesList.add(doubleValue);
            longsList.add(longValue);
        }

        double regularDoublesStd = computeStdDevDoubles(doublesList);
        long regularLongsStd = computeStdDevLongs(longsList);

        StaticLongs longs = new StaticLongs(longsList);
        StaticDoubles doubles = new StaticDoubles(doublesList);

        Std agg = new Std();
        double runningDoublesStd = agg.runDouble(doubles);
        long runningLongsStd = agg.runLong(longs);

        //System.out.println("regular-doubles-std: " + regularDoublesStd);
        //System.out.println("running-doubles-std: " + runningDoublesStd);
        //System.out.println("regular-longs-std: " + regularLongsStd);
        //System.out.println("running-longs-std: " + runningLongsStd);

        // Calculate the epsilon based on the percentage of the number
        double epsilon = (EPSILON_PERCENTAGE * regularLongsStd);

        // Sanity check, the regular calculated numbers should meet the required conditions
        Assert.assertTrue(Math.abs(regularLongsStd - regularDoublesStd) < epsilon);

        // Check for equivalence of samples vs expected values by an epsilon
        Assert.assertTrue(Math.abs(regularDoublesStd - runningDoublesStd) < epsilon);
        Assert.assertTrue(Math.abs(regularLongsStd - runningLongsStd) < epsilon);
        Assert.assertTrue(Math.abs(runningLongsStd - runningDoublesStd) < epsilon);
    }

    public double computeStdDevDoubles(List<Double> doubles) {
        double sum = 0;
        double mean = 0;

        for(double value : doubles) {
            sum += value;
        }
        mean = sum / doubles.size();

        double squaresum = 0;
        for(double value : doubles) {
            squaresum += Math.pow(value-mean, 2);
        }
        double variance = squaresum / doubles.size();
        double stdDev = Math.sqrt(variance);
        return stdDev;
    }


    public long computeStdDevLongs(List<Long> longs) {
        double sum = 0;
        double mean = 0;

        for(double value : longs) {
            sum += value;
        }
        mean = sum / longs.size();

        double squaresum = 0;
        for(double value : longs) {
            squaresum += Math.pow(value-mean, 2);
        }
        double variance = squaresum / longs.size();
        double stdDev = Math.sqrt(variance);
        return (long) stdDev;
    }

}

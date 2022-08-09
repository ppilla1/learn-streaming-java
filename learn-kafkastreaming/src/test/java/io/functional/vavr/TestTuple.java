package io.functional.vavr;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Log4j2
public class TestTuple {

    @Test
    public void test_create_tuple() {
        Tuple2<String, Integer> java8 = Tuple.of("Java", 8);
        assertEquals("Java", java8._1);
        assertEquals(8, java8._2);
    }

    @Test
    public void test_map_tuple_component_wise() {
        Tuple2<String, Integer> java8 = Tuple.of("Java", 8);
        Tuple2<String, Integer> that = java8.map(s -> s.substring(2) + "vr", i -> i / 8);
        assertEquals("vavr", that._1);
        assertEquals(1, that._2);
    }

    @Test
    public void test_map_tuple_one_mapper() {
        Tuple2<String, Integer> java8 = Tuple.of("Java", 8);
        Tuple2<String, Integer> that = java8.map((s, i) -> Tuple.of(s.substring(2) + "vr", i / 8));
        assertEquals("vavr", that._1);
        assertEquals(1, that._2);
    }

    @Test
    public void test_transform_tuple() {
        Tuple2<String, Integer> java8 = Tuple.of("Java", 8);
        String that = java8.apply((s, i) -> s.substring(2) + "vr" + i / 8);
        assertEquals("vavr1", that);
    }
}

package com.actuallygr.mapper;

import com.actuallygr.pojos.House;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;

public class HouseMapper implements MapFunction<Row, House> {
    @Override
    public House call(Row row) throws Exception {
        House house = new House();
        house.setId(row.getAs("id"));
        house.setAddress(row.getAs("address"));
        house.setSqft(row.getAs("sqft"));
        house.setPrice(row.getAs("price"));

        String vacantByString = row.getAs("vacantBy").toString();
        if(vacantByString != null) {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
            house.setVacantBy(parser.parse(vacantByString));
        }
        return house;
    }
}

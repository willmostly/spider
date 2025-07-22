#!/usr/bin/env bash




# For use with Minio.
INSTANCE=local
BUCKET=demo
FLIGHT_SCHEMA_PATH=flight_2
CAR_SCHEMA_PATH=car_1

AIRLINE_DATA_PATH=/database/flight_2/data_csv/
mc cp ${AIRLINE_DATA_PATH}/airlines.csv ${INSTANCE}/${BUCKET}/${FLIGHT_SCHEMA_PATH}/airlines/
mc cp ${AIRLINE_DATA_PATH}/airports100.csv ${INSTANCE}/${BUCKET}/${FLIGHT_SCHEMA_PATH}/airports/
mc cp ${AIRLINE_DATA_PATH}/flights.csv ${INSTANCE}/${BUCKET}/${FLIGHT_SCHEMA_PATH}/flights/

CAR_DATA_PATH=database/car_1/data_csv
mc cp ${CAR_DATA_PATH}/car-makers.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/car_makers/
mc cp ${CAR_DATA_PATH}/car-names.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/car_names.csv/
mc cp ${CAR_DATA_PATH}/cars-data.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/cars_data/
mc cp ${CAR_DATA_PATH}/countries.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/countries/
mc cp ${CAR_DATA_PATH}/model-list.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/model-list/
mc cp ${CAR_DATA_PATH}/continents.csv ${INSTANCE}/${BUCKET}/${CAR_SCHEMA_PATH}/continents/

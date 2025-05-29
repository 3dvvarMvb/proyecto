data = LOAD '/data/input.csv' USING PigStorage(',') AS (field1:int, field2:chararray);
filtered_data = FILTER data BY field1 > 100;
DUMP filtered_data;
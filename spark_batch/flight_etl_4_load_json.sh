bucket=gs://spark-etl-1
current_date=$(date -v -1d +"%Y-%m-%d")

bq load --source_format=NEWLINE_DELIMITED_JSON \
data_analysis.avg_delays_by_distance_category \
$bucket/flights_data_output/${current_date}"_distance_category/*.json" &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
data_analysis.avg_delays_by_flight_nums \
$bucket/flights_data_output/${current_date}"_flight_nums/*.json"

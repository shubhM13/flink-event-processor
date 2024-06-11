from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.table import StreamTableEnvironment
from pyflink.table.descriptors import Schema, Kafka, Json

# Initialize environments
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Define Kafka sources
view_kafka_source = Kafka()
view_kafka_source.version('universal')
view_kafka_source.topic('views')
view_kafka_source.start_from_earliest()
view_kafka_source.property('bootstrap.servers', 'localhost:9092')
view_kafka_source.property('group.id', 'view_group')

click_kafka_source = Kafka()
click_kafka_source.version('universal')
click_kafka_source.topic('clicks')
click_kafka_source.start_from_earliest()
click_kafka_source.property('bootstrap.servers', 'localhost:9092')
click_kafka_source.property('group.id', 'click_group')

# Define the table schema for the sources
view_schema = Schema()
view_schema.field('ID', Types.STRING())
view_schema.field('AdName', Types.STRING())
view_schema.field('SiteName', Types.STRING())
view_schema.field('Time', Types.SQL_TIMESTAMP())

click_schema = Schema()
click_schema.field('ID', Types.STRING())
click_schema.field('AdName', Types.STRING())
click_schema.field('SiteName', Types.STRING())
click_schema.field('Time', Types.SQL_TIMESTAMP())

# Register Kafka sources as tables
table_env.connect(view_kafka_source).with_format(Json()).with_schema(view_schema).in_append_mode().create_temporary_table('views')
table_env.connect(click_kafka_source).with_format(Json()).with_schema(click_schema).in_append_mode().create_temporary_table('clicks')

# SQL queries to compute metrics
queries = {
    "total_views": """
        SELECT AdName, SiteName, COUNT(*) as total_views
        FROM views
        GROUP BY AdName, SiteName
    """,
    "total_clicks": """
        SELECT AdName, SiteName, COUNT(*) as total_clicks
        FROM clicks
        GROUP BY AdName, SiteName
    """,
    "views_sliding_window": """
        SELECT TUMBLE_START(Time, INTERVAL '2' HOUR) as window_start,
               AdName, SiteName, COUNT(*) as views_count
        FROM views
        GROUP BY TUMBLE(Time, INTERVAL '2' HOUR), AdName, SiteName
    """,
    "clicks_per_view": """
        SELECT v.AdName, v.SiteName, v.ID, COUNT(c.ID) as click_count
        FROM views v LEFT JOIN clicks c ON v.ID = c.ID
        GROUP BY v.AdName, v.SiteName, v.ID
    """,
    "average_clicks_per_view": """
        SELECT AdName, SiteName, AVG(click_count) as avg_clicks
        FROM (
            SELECT v.AdName, v.SiteName, v.ID, COUNT(c.ID) as click_count
            FROM views v LEFT JOIN clicks c ON v.ID = c.ID
            GROUP BY v.AdName, v.SiteName, v.ID
        ) GROUP BY AdName, SiteName
    """,
    "clicks_histogram": """
        SELECT AdName, SiteName, click_count, COUNT(*) as histogram_count
        FROM (
            SELECT v.AdName, v.SiteName, COUNT(c.ID) as click_count
            FROM views v LEFT JOIN clicks c ON v.ID = c.ID
            GROUP BY v.AdName, v.SiteName
        ) GROUP BY AdName, SiteName, click_count
    """
}

# Execute queries and write results to Kafka
for metric, query in queries.items():
    result_table = table_env.sql_query(query)
    result_kafka_sink = Kafka()
    result_kafka_sink.version('universal')
    result_kafka_sink.topic('metrics')
    result_kafka_sink.property('bootstrap.servers', 'localhost:9092')
    result_kafka_sink.with_format(Json()).with_schema(Schema().field('value', Types.STRING()))
    
    table_env.connect(result_kafka_sink).with_format(Json()).with_schema(Schema().field('value', Types.STRING())).in_append_mode().create_temporary_table(f'{metric}_sink')
    result_table.insert_into(f'{metric}_sink')

env.execute("View Click Processor")

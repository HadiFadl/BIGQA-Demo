from neo4j import GraphDatabase
from ksql import KSQLAPI
import logging

# Function to retrieve derived measures from Neo4j based on context name
def retrieve_derived_measures(context_name):
    derived_measures = {}

    # Connect to Neo4j
    uri = "bolt://localhost:7687"  # Update with the appropriate URI
    username = "neo4j"  # Update with the appropriate username
    password = "password"  # Update with the appropriate password
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        # Query Neo4j to retrieve derived measures, base measures, and weights based on context name
        result = session.run(
            "MATCH (c:Context {name: $contextName})-[:HAS_DERIVED_MEASURE]->(dm:DerivedMeasure)"
            "-[:BASED_ON]->(bm:BaseMeasure) "
            "RETURN dm.name AS derivedMeasure, bm.name AS baseMeasure, dm.weight AS weight",
            contextName=context_name
        )

        # Parse the retrieved data
        for record in result:
            derived_measure = record["derivedMeasure"]
            base_measure = record["baseMeasure"]
            weight = record["weight"]

            if derived_measure not in derived_measures:
                derived_measures[derived_measure] = {}

            derived_measures[derived_measure][base_measure] = weight

    # Close the Neo4j driver
    driver.close()

    return derived_measures

# Function to build the KSQL command for creating a table with derived measures
def build_table_command(table_name, base_measures, kafka_topic,  identifier_attribute, window_start, window_end, derived_measures):
    command = f"CREATE TABLE {table_name} AS SELECT {window_start} as Window_Start, {window_end} as Window_END, "
    for measure, weight in derived_measures.items():
        command += f" {weight} * {measure} as {measure} +"
        
    command = command[:-1]
    command += f" FROM {kafka_topic} WINDOW TUMBLING (SIZE 5 seconds, RETENTION 10 minutes, GRACE PERIOD 5 minutes) GROUP BY {identifier_attribute} EMIT FINAL ;"
    return command

# Function to retrieve data quality assessment plan from Neo4j and build KSQL commands
def build_ksql_commands(context_name, kafka_topic, identifier_attribute):
    command_list = []
    # Retrieve data quality assessment plan from Neo4j
    derived_measures = retrieve_derived_measures(context_name)  # Call the function to retrieve derived measures from Neo4j

    # Build KSQL commands for creating tables and calculating derived measures
    for measure, base_measures in derived_measures.items():
        command = build_table_command(measure,base_measures,kafka_topic,identifier_attribute, "from_unixtime(WINDOWSTART)", "from_unixtime(WINDOWEND)", base_measure_strings)
        command_list.append(command)

    return command_list
    


logging.basicConfig(level=logging.DEBUG)
client = KSQLAPI('http://localhost:8088')

context_name = 'Radiation Monitoring'  # Update with the desired context name
ksql_commands = build_ksql_commands(context_name, "sensor", "SNESOR_ID")

# Execute the KSQL commands
for command in ksql_commands:
    client.ksql(command)

# Other KSQL commands
client.ksql('show tables')
client.ksql('show streams')
client.ksql('list functions')
a = client.query('Select * from NNT')

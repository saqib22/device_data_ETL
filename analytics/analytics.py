from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

import json
from datetime import datetime
from geopy.distance import geodesic
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, func


def calculate_distance(location1, location2):
    lat1, lon1 = location1['latitude'], location1['longitude']
    lat2, lon2 = location2['latitude'], location2['longitude']
    distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
    return distance


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to Source and Sink successful.')

#Create table for storing aggregated data in MySQL
while True:
    try:
        metadata_obj = MetaData()
        aggregated_data = Table(
            'aggregated_data', metadata_obj,
            Column('device_id', String(128)),
            Column('hour', String(128)),
            Column('max_temperature', Float),
            Column('data_points_count', Integer),
            Column('total_distance', Float),
        )
        metadata_obj.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)


# Create SQLAlchemy sessions
psql_Session = sessionmaker(bind=psql_engine)
mysql_Session = sessionmaker(bind=mysql_engine)

# Pull data from PostgreSQL, perform aggregations, calculate distance, and store in MySQL
with psql_Session() as pg_session, mysql_Session() as mysql_session:
    rows = pg_session.execute(text('SELECT device_id, temperature, location, time FROM devices;'))

    for row in rows:
        device_id, temperature, location, timestamp = row

        prev_location = pg_session.execute(text('''
            SELECT location, time
            FROM devices
            WHERE device_id = :device_id AND time < :timestamp
            ORDER BY time DESC
            LIMIT 1
        '''), {"device_id": device_id, "timestamp": timestamp}).fetchone()

        timestamp = datetime.fromtimestamp(int(timestamp))

        # Calculate the hour
        hour = timestamp.replace(minute=0, second=0, microsecond=0)

        # Calculate the maximum temperature per hour
        mysql_session.execute(text('''
            INSERT INTO aggregated_data (device_id, hour, max_temperature, data_points_count, total_distance)
            VALUES (:device_id, :hour, :temperature, 0, 0)
            ON DUPLICATE KEY UPDATE
                max_temperature = GREATEST(max_temperature, :temperature)
        '''), {"device_id": device_id, "hour": hour, "temperature": temperature, "distance": 0})
        
        mysql_session.commit()

        # Update the data_points_count per hour
        mysql_session.execute(text('''
            UPDATE aggregated_data
            SET data_points_count = data_points_count + 1
            WHERE device_id = :device_id AND hour = :hour
        '''), {"device_id": device_id, "hour": hour})

        mysql_session.commit()

        # Calculate the total distance per hour
        if prev_location:
            distance = calculate_distance(
                json.loads(prev_location[0]),
                json.loads(location)
            )
            mysql_session.execute(text('''
                UPDATE aggregated_data
                SET total_distance = total_distance + :distance
                WHERE device_id = :device_id AND hour = :hour
            '''), {"device_id": device_id, "hour": hour, "distance": distance})

        mysql_session.commit()

# Close connections
psql_engine.dispose()
mysql_engine.dispose()

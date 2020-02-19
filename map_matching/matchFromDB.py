#!/usr/bin/env python3

import requests
import json
import pandas as pd
from datetime import timedelta, date, datetime
# Replace with your own data, db is just our database connection
import db


def stringify(data):
    result = ''

    if isinstance(data, pd.Series):
        result = ';'.join(map(str, data.values.tolist()))
    elif isinstance(data, pd.DataFrame):
        result = ';'.join([','.join(ele.split()) for ele in data.to_string(
            header=False, index=False, index_names=False).split('\n')])
    else:
        raise Exception(
            'Expected a pandas dataframe or series. Got: {}', format(type(data)))

    return result.replace(' ', '')


def get_timestamp(series):
    if not isinstance(series, pd.Series):
        raise Exception('Expected a pandas series. Got: {}',
                        format(type(series)))

    d, t = series[['LokalDato', 'LokalTid']]
    return int(datetime.combine(d, t).timestamp())


def process_match(match, trip):
    matchings_indexes, matchings = zip(
        *[(index, matching) for index, matching in enumerate(match['matchings']) if matching['confidence'] > 0.85])
    tracepoints_indexes, tracepoints = zip(
        *[(index, tracepoint) for index, tracepoint in enumerate(match['tracepoints']) if tracepoint != None and tracepoint['matchings_index'] in matchings_indexes])
    trip = trip.iloc[list(tracepoints_indexes)].reset_index(drop=True)

    return matchings, tracepoints, trip


def raw_match(df):
    coordinates = stringify(df[['Lon', 'Lat']])
    timestamps = stringify(df['Timestamp'])

    response = requests.get(
        f"http://127.0.0.1:5000/match/v1/driving/{coordinates}?overview=full&annotations=true&geometries=geojson&timestamps={timestamps}")

    if not response.ok:
        code = response.json()['code']

        if code == 'NoSegment' or code == 'NoMatch':
            return None
        else:
            raise Exception(
                f"{response.json()} - at timestamp: {df['Timestamp'].iloc[0]}")
    else:
        return response.json()


def get_trip_match(start=date(2012, 11, 19), end=None):
    delta = timedelta(days=1)

    while True:
        table_name = f'Trip2PosRap_{start.strftime("%Y%m%d")}'

        if not db.exists(table_name):
            break
        
        df = db.query(f'SELECT * FROM public."{table_name}"')

        print(f'\n{datetime.now()} - {len(df.index)} rows fetched from {table_name}')
                
        # Change this such that your df has the the right column-names. 
        df["Timestamp"] = df['LokalTid'].map(lambda t: int(datetime.combine(start, t).timestamp()))

        start += delta

        # Skip if number of rows is below threshold
        if df.shape[0] < 32:
            continue

        # Group by TripId
        trips = df.groupby(by='TripId', as_index=False)

        print(f'{datetime.now()} - table preprocessing complete')

        for _, trip in trips:
            match = raw_match(trip)

            if not match or not [matching for matching in match['matchings'] if matching['confidence'] > 0.85]:
                continue

            yield process_match(match, trip)
        

        if (end != None and end <= start):
            break



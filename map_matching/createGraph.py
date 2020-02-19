import ast
import networkx as nx
import pandas as pd
import osmnx as ox
import pickle
import pathlib
import warnings
import time
from datetime import datetime
from matchFromDB import get_trip_match
import db

_intersections = None
_road_network = None


def get_intersections():
    global _intersections
    if not _intersections:
        with open('denmarkOsmNodes.txt', 'r') as f:
            _intersections = ast.literal_eval(f.read())

    return _intersections


def get_road_network(location='Denmark') -> nx.MultiDiGraph:
    global _road_network

    if not _road_network and not pathlib.Path('graph.pickle').exists():
        _road_network = ox.graph_from_place(
            location, network_type='drive', retain_all=True, simplify=True)
        f = open('graph.pickle', 'wb')
        pickle.dump(_road_network, f)
    elif not _road_network:
        f = open('graph.pickle', 'rb')
        _road_network = pickle.load(f)

    return _road_network


def aggregate(trip, matchings):
    trip_index = 0
    rows = []
    start_times = []

    # Enumerate the matchings (http://project-osrm.org/docs/v5.5.1/api/#route-object)
    for match_index, match in enumerate(matchings):

        # Store the start time from the original trip data for each matching
        start_times.append(trip.at[trip_index, 'Timestamp'])

        # Enumerate the legs of each matching (http://project-osrm.org/docs/v5.5.1/api/#routeleg-object)
        for _, leg in enumerate(match['legs']):
            annotation = leg['annotation']
            nodes = annotation['nodes']

            # Calculate the travel time from the original trip data
            travel_time = trip.at[trip_index + 1,
                                  'Timestamp'] - trip.at[trip_index, 'Timestamp']

            """
                1) Check if there are more than two OSM nodes along the routeleg and if so, divide the travel time evenly
                2) Round the travel time to three decimal places to get around rounding error when converting to date time later
                3) Construct pairs of nodes (edges)
            """
            if len(nodes) > 2:
                travel_time = travel_time / (len(nodes) - 1)
                for j, node in enumerate(nodes[1:]):
                    rows.append(
                        [match_index, (nodes[j], node), round(travel_time, 3)])
            else:
                rows.append(
                    [match_index, (nodes[0], nodes[1]), round(travel_time, 3)])

            # Count up the trip index (1:1 correspondence between the original trip data and the number of routelegs)
            trip_index += 1

        # Skip connecting leg
        trip_index += 1

    # Group by edge and match id and summarize the travel time to remove redundant rows
    graph = pd.DataFrame(rows, columns=['MatchId', 'Edge', 'TravelTime']).groupby(['MatchId', 'Edge'], as_index=False, sort=False)[
        'TravelTime'].sum()

    # Enumerate the start times stored earlier and construct the date time for each row by adding the travel time timedelta
    for match_index, timestamp in enumerate(start_times):
        dt = pd.Timestamp(timestamp, unit='s') + pd.Timedelta(2, unit='h')

        for i in graph.index[graph['MatchId'] == match_index]:
            graph.at[i, 'DateTime'] = dt
            dt += pd.Timedelta(graph.at[i, 'TravelTime'], unit='s')

    return graph


def topologize(graph, intersections, trip_id, boks_id):
    rows = []
    travel_time = 0
    source = None

    # Iterate the graph rows as tuples
    for row in graph.itertuples():

        # Aggregate the travel time
        travel_time += row.TravelTime

        # Set a source row that has a starting node in an intersection
        # Iterate till a destination row is found with a matching match id and a destination node in an intersection
        # If the match id of the source and destination does not match, set the source to none and restart the process
        if not source and row.Edge[0] in intersections:
            source = row
            travel_time = row.TravelTime

        if source and row.Edge[1] in intersections:
            if source.MatchId != row.MatchId:
                source = None
                continue

            if source.Edge[0] != row.Edge[1]:
                rows.append([trip_id, boks_id, source.MatchId, source.Edge[0],
                             row.Edge[1], travel_time, source.DateTime])
                source = None

    return pd.DataFrame(rows, columns=['TripId', 'BoksId', 'MatchId', 'Source', 'Destination', 'TravelTime', 'DateTime'])


def get_distance(series: pd.Series):
    if not isinstance(series, pd.Series):
        raise Exception('Expected a pandas series. Got: {}',
                        format(type(series)))

    rn = get_road_network()
    src, dest = series[['Source', 'Destination']]
    edge_data = rn.get_edge_data(src, dest)
    distance = edge_data[0]['length'] if edge_data else None
    return distance


def create_graph():
    buffer = pd.DataFrame()

    for matchings, _, trip in get_trip_match():
        start = time.process_time()

        graph = aggregate(trip, matchings)

        print(f'aggregate - {time.process_time() - start}')

        graph = topologize(graph, get_intersections(),
                           trip['TripId'].iloc[0], trip['BoksId'].iloc[0])

        print(f'topologize - {time.process_time() - start}')

        if len(graph) == 0:
            continue

        if buffer.empty:
            buffer = graph
            print('empty')
        elif graph['DateTime'].iloc[0].strftime('%x') != buffer['DateTime'].iloc[0].strftime('%x'):
            buffer['Distance'] = buffer[['Source', 'Destination']].apply(
                func=get_distance, axis=1)
            db.insert('test', buffer, replace=True)
            buffer = graph
            print('insertion')
            exit()
        else:
            buffer = buffer.append(graph)
            print('append')

        #table_name: str = graph['DateTime'].iloc[0].strftime('%Y%m')
        #db.insert(table_name, graph, replace=False)

        #db.insert('test', graph, replace=True)

        # print(
        #    f"{datetime.now()} - topologized trip {graph['TripId'].iloc[0]} inserted into table '{table_name}'")
        #graph.to_csv("Datafile1.csv", header=False, index=False)


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    create_graph()

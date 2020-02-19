import unittest
import pandas as pd
from createGraph import aggregate, topologize
from datetime import datetime, date, time
from pandas.util.testing import assert_frame_equal



class testCreateGraph(unittest.TestCase):

    def test_aggregate(self):
        # Arrange
        t = 1570406400

        # Input for aggregate
        matchings = [
            {"legs":[{"annotation":{"nodes":[1, 2, 3]}},
            {"annotation":{"nodes":[2, 3]}},
            {"annotation":{"nodes":[2, 3]}},
            {"annotation":{"nodes":[2, 3, 4]}},
            {"annotation":{"nodes":[4, 5]}}]},
            {"legs":[{"annotation":{"nodes":[10, 11]}},
            {"annotation":{"nodes":[10, 11]}},
            {"annotation":{"nodes":[11, 12]}}]}
        ]

        trip = pd.DataFrame([t, t+2, t+3, t+4, t+6, t+7, t, t+1, t+2, t+3], columns=['Timestamp'])


        # Expected output
        expected = pd.DataFrame(
            [
                (0, (1, 2), 1, pd.Timestamp(t + (3600 * 2), unit='s')),
                (0, (2, 3), 4, pd.Timestamp(t+1 + (3600 * 2), unit='s')),
                (0, (3, 4), 1, pd.Timestamp(t+5 + (3600 * 2), unit='s')),
                (0, (4, 5), 1, pd.Timestamp(t+6 + (3600 * 2), unit='s')),
                (1, (10, 11), 2, pd.Timestamp(t + (3600 * 2), unit='s')),
                (1, (11, 12), 1, pd.Timestamp(t+2 + (3600 * 2), unit='s')),
            ],columns=['MatchId', 'Edge', 'TravelTime', 'DateTime'])

        # Act
        result = aggregate(trip, matchings)

        # Assert
        assert_frame_equal(result, expected, check_dtype=False)


        
    
    def test_topologize(self):
        # Arrange
        timestamp = pd.Timestamp(1570406400, unit='s')

        
        # Input for topologize
        intersections = [3, 5, 8, 13, 14, 16, 18, 20]
        
        data = [
            [0, (1, 2), 2, timestamp],
            [0, (2, 3), 2, timestamp],
            [0, (3, 4), 2, timestamp],
            [0, (4, 5), 2, timestamp],
            [0, (5, 6), 2, timestamp],
            [1, (7, 8), 2, timestamp],
            [1, (7, 8), 2, timestamp],
            [1, (8, 9), 2, timestamp],
            [1, (9, 10), 2, timestamp],
            [1, (10, 11), 2, timestamp],
            [1, (11, 12), 2, timestamp],
            [1, (12, 13), 2, timestamp],
            [1, (13, 13), 2, timestamp],
            [1, (13, 14), 2, timestamp],
            [2, (15, 16), 2, timestamp],
            [2, (16, 17), 2, timestamp],
            [2, (17, 18), 2, timestamp],
            [2, (18, 19), 2, timestamp],
            [2, (19, 20), 2, timestamp]

        ]

        aggregated_edges = pd.DataFrame(data, columns=['MatchId', 'Edge', 'TravelTime', 'DateTime'])

        # Expected output
        expected = pd.DataFrame([
                ["TripID", "BoksID", 0, 3, 5, 4, timestamp], 
                ["TripID", "BoksID", 1, 8, 13, 10, timestamp], 
                ["TripID", "BoksID", 1, 13, 14, 4, timestamp], 
                ["TripID", "BoksID", 2, 16, 18, 4, timestamp], 
                ["TripID", "BoksID", 2, 18, 20, 4, timestamp]
            ], 
            columns=['TripId', 'BoksId', 'MatchId', 'Source', 'Destination', 'TravelTime', 'DateTime'])

        # Act
        result = topologize(aggregated_edges, intersections, "TripID", "BoksID")

        # Assert
        assert_frame_equal(result, expected, check_dtype=False)


if __name__ == '__main__':
    unittest.main()
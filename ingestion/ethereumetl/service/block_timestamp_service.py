# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import itertools
from datetime import datetime, timezone


class BlockTimestampService(object):
    def __init__(self, web3):
        graph = BlockTimestampGraph(web3)
        self._graph_operations = TimeGraphSearchService(graph)

    def get_block_range_for_date(self, date):
        start_datetime = datetime.combine(date, datetime.min.time().replace(tzinfo=timezone.utc))
        end_datetime = datetime.combine(date, datetime.max.time().replace(tzinfo=timezone.utc))
        return self.get_block_range_for_timestamps(start_datetime.timestamp(), end_datetime.timestamp())

    def get_block_range_for_timestamps(self, start_timestamp, end_timestamp):
        start_timestamp = int(start_timestamp)
        end_timestamp = int(end_timestamp)
        if start_timestamp > end_timestamp:
            raise ValueError("start_timestamp must be lesser than end_timestamp")

        try:
            start_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(start_timestamp)
        except OutOfBoundsError:
            start_block_bounds = (0, 0)

        try:
            end_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(end_timestamp)
        except OutOfBoundsError as e:
            raise OutOfBoundsError("The existing blocks do not completely cover the given time range") from e

        if start_block_bounds == end_block_bounds and start_block_bounds[0] != start_block_bounds[1]:
            raise ValueError("The given timestamp range does not cover any blocks")

        start_block = start_block_bounds[1]
        end_block = end_block_bounds[0]

        # The genesis block has timestamp 0 but we include it with the 1st block.
        if start_block == 1:
            start_block = 0

        return start_block, end_block


class BlockTimestampGraph(object):
    def __init__(self, web3):
        self._web3 = web3

    def get_first_point(self):
        # Ignore the genesis block as its timestamp is 0
        return block_to_point(self._web3.eth.get_block(1))

    def get_last_point(self):
        return block_to_point(self._web3.eth.get_block("latest"))

    def get_point(self, x):
        return block_to_point(self._web3.eth.get_block(x))


def block_to_point(block):
    return Point(block.number, block.timestamp)


class TimeGraphSearchService(object):
    def __init__(self, graph):
        """x axis on the graph must be integers, y value must increase strictly monotonically with increase of x"""
        self._graph = graph
        self._cached_points = []

    def get_bounds_for_y_coordinate(self, y):
        """given the y coordinate, outputs a pair of x coordinates for closest points that bound the y coordinate.
        Left and right bounds are equal in case given y is equal to one of the points y coordinate"""
        initial_bounds = find_best_bounds(y, self._cached_points)
        if initial_bounds is None:
            initial_bounds = self._get_first_point(), self._get_last_point()

        result = self._get_bounds_for_y_coordinate_recursive(y, *initial_bounds)
        return result

    def _get_bounds_for_y_coordinate_recursive(self, y, start, end):
        if y < start.y or y > end.y:
            raise OutOfBoundsError("y coordinate {} is out of bounds for points {}-{}".format(y, start, end))

        if y == start.y:
            return start.x, start.x
        elif y == end.y:
            return end.x, end.x
        elif (end.x - start.x) <= 1:
            return start.x, end.x
        else:
            assert start.y < y < end.y
            if start.y >= end.y:
                raise ValueError("y must increase strictly monotonically")

            estimation1_x = interpolate(start, end, y)
            estimation1_x = bound(estimation1_x, (start.x, end.x))
            estimation1 = self._get_point(estimation1_x)

            if estimation1.y < y:
                points = (start, estimation1)
            else:
                points = (estimation1, end)

            estimation2_x = interpolate(*points, y)
            estimation2_x = bound(estimation2_x, (start.x, end.x))
            estimation2 = self._get_point(estimation2_x)

            all_points = [start, estimation1, estimation2, end]

            bounds = find_best_bounds(y, all_points)
            if bounds is None:
                raise ValueError("Unable to find bounds for points {} and y coordinate {}".format(points, y))

            return self._get_bounds_for_y_coordinate_recursive(y, *bounds)

    def _get_point(self, x):
        point = self._graph.get_point(x)
        self._cached_points.append(point)
        return point

    def _get_first_point(self):
        point = self._graph.get_first_point()
        self._cached_points.append(point)
        return point

    def _get_last_point(self):
        point = self._graph.get_last_point()
        self._cached_points.append(point)
        return point


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b, strict=False)


def find_best_bounds(y, points):
    sorted_points = sorted(points, key=lambda point: point.y)
    for point1, point2 in pairwise(sorted_points):
        if point1.y <= y <= point2.y:
            return point1, point2
    return None


def interpolate(point1, point2, y):
    x1, y1 = point1.x, point1.y
    x2, y2 = point2.x, point2.y
    if y1 == y2:
        raise ValueError("The y coordinate for points is the same {}, {}".format(point1, point2))
    x = int((y - y1) * (x2 - x1) / (y2 - y1) + x1)
    return x


def bound(x, bounds):
    x1, x2 = bounds
    if x1 > x2:
        x1, x2 = x2, x1
    if x <= x1:
        return x1 + 1
    elif x >= x2:
        return x2 - 1
    else:
        return x


class OutOfBoundsError(Exception):
    pass


class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __str__(self):
        return "({},{})".format(self.x, self.y)

    def __repr__(self):
        return "Point({},{})".format(self.x, self.y)

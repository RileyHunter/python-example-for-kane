import pandas as pd
from itertools import count
from numpy import random
auto_id = count()

# Adjacency matrix, values are None for no connection
# or a value for latency ms
network = [
    [None, 100, None, None],
    [100, None, 115, 315],
    [None, 115, None, 80],
    [None, 315, 80, None]
]

def find_shortest_routes(network):
    routes = {}
    def bfs(start, end):
        queue = [[start]]
        best = None
        while len(queue) > 0:
            history = queue.pop()
            latest_node = history[-1]
            for next_node in range(len(network)):
                if network[latest_node][next_node] is None:
                    continue
                if next_node in history:
                    continue
                if next_node == end:
                    if best is None or len(history)+1 < len(best):
                        best = history + [next_node]
                else:
                    queue.append([_ for _ in history] + [next_node])
        return best
    for start in range(len(network)):
        for end in range(len(network)):
            if start == end:
                continue
            routes[(start, end)] = bfs(start, end)
    return routes

def send_message(start, end, network, routes):
    message_id = next(auto_id)
    route = routes[(start, end)]
    rows = []
    for i in range(len(route)-1):
        current_node = route[i]
        next_node = route[i+1]
        edge_latency = network[current_node][next_node]
        final_latency = random.normal(loc=edge_latency, scale=edge_latency*.1)
        rows.append([message_id, current_node, next_node, final_latency])
    return rows

def send_many_messages(num_messages, network, routes):
    rows = []
    for i in range(num_messages):
        start = random.choice(len(network))
        end = random.choice(len(network))
        while end == start:
            end = random.choice(len(network))
        rows.extend(send_message(start, end, network, routes))
    return rows

def get_dataframe(num_rows):
    return pd.DataFrame(
        send_many_messages(num_rows, network, find_shortest_routes(network)),
        columns=['message_id', 'from_node', 'to_node', 'latency']
    )
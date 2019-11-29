import sys
import pandas as pd


def filter_at_client(query, result):

    returned_table = pd.DataFrame(result)
    print("Returned by the contractor")
    print(returned_table)

    if 'aggregation' in query and 'where' in query:
        res = where_filter(query['where'], result, returned_table)
        print(agg_filter(query, result, res))
    elif 'where' in query and 'aggregation' not in query:
        print(where_filter(query['where'], result, returned_table))
    elif 'aggregation' in query and 'where' not in query:
        print(agg_filter(query, result, returned_table))


def where_filter(where_query, result, returned_table):

    filter_list = []
    link_op = where_query.get('link_operation', 'or')
    if link_op == 'and':
        df = True
    else:
        df = False

    for criteria in where_query['match_criteria']:
        col_name = criteria['column_name']
        target_value = criteria['attributes']['value']
        equality = criteria['attributes']['matching_type']
        filter_list.append((col_name,equality,target_value))

    for condition in filter_list:
        if condition[1] == 'starts_with':
            if link_op == 'and':
                df = df & (returned_table[condition[0]].str.startswith(condition[2]))
            else:
                df = df | (returned_table[condition[0]].str.startswith(condition[2]))
        else:
            rows = returned_table[condition[0]].astype(int)
            if condition[1] == 'greater_than':
                if link_op == 'and':
                    df = df & (rows > condition[2])
                else:
                    df = df | (rows > condition[2])
            elif condition[1] == 'lesser_than':
                if link_op == 'and':
                    df = df & (rows < condition[2])
                else:
                    df = df | (rows < condition[2])
            elif condition[1] == 'equals':
                if link_op == 'and':
                    df = df & (rows == condition[2])
                else:
                    df = df | (rows == condition[2])

    returned_table = returned_table[df]
    return returned_table


def agg_filter(query, value_agg, returned_table):

    if query['aggregation']['function'] == "count":
        return len(value_agg)

    if query['aggregation']['function'] == "min":
        col_name = query['aggregation']['column_name']
        return returned_table[col_name].min()

    if query['aggregation']['function'] == "max":
        col_name = query['aggregation']['column_name']
        return returned_table[col_name].max()

    if query['aggregation']['function'] == "sum":
        col_name = query['aggregation']['column_name']
        return returned_table[col_name].astype(int).sum()

    if query['aggregation']['function'] == "avg":
        col_name = query['aggregation']['column_name']
        return returned_table[col_name].astype(int).mean()


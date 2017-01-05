import os
import json
import pandas as pd
import numpy as np


def important_print(message, level=0):
    if level == 1:
        print("-" * 70)
        print(message.capitalize())
        print("-" * 70)
    if level == 0:
        print("-" * 50)
        print(message)
        print("-" * 50)


def flattenjson(b, delim):
    val = {}
    if isinstance(b, dict):
        for i in b.keys():
            if isinstance(b[i], dict):
                get = flattenjson(b[i], delim)
                for j in get.keys():
                    val[i + delim + j] = get[j]
            else:
                val[i] = b[i]
    return val


def length_with_nan(x):
    if isinstance(x, list):
        return len(x)
    else:
        return x


def first_with_nan(x):
    if isinstance(x, list):
        return x[0]
    else:
        return x


def keys_with_nan(x):
    if isinstance(x, dict):
        return str(list(x.keys()))
    else:
        return x


def check_asked_cols(df_to_check, cols_to_check):
    real_columns = df_to_check.columns
    for column in cols_to_check:
        if column not in real_columns:
            if debug:
                print("Column " + column +
                      " is not a real column in the dataframe.")
            cols_to_check.remove(column)
    return cols_to_check


def flatten_columns(df, columns_list, drop=False, debug=False):
    """
    Goal: flatten a dataframe, which has columns filled with lists or dictionnaries.

    Parameters:
    - df : dataframe: the dataframe you want to flatten
    - columns_list: list of strings: the columns you want to flatten
    - drop: boolean: to drop flattened columns

    Dataframe is changed in place, with new columns, and flattened columns deleted if drop=True.
    Returns added columns
    """

    # First, we check if asked columns are present
    columns_list = check_asked_cols(df, columns_list)

    # Instanciate list of created columns during process
    added_columns = []
    for column_flattened in columns_list:
        if debug:
            print("-" * 20)
            print("FLATTENING " + column_flattened.capitalize())

        # Find out whether the value is a dict
        # TODO check on whole column
        if isinstance(df[column_flattened][0], dict):

            # Lets check if keys are all the same:
            # First we find keys on the dictionnary
            series_keylists = df[column_flattened].apply(keys_with_nan)
            # We count occurences of unique lists of keys
            keylists_counts = series_keylists.value_counts()

            # If there is always the same key we can flatten
            if len(keylists_counts) == 1:
                if debug:
                    print("This columns has dicts of same keys. Ok to flatten.")
                keys_to_flatten = list(df[column_flattened][0].keys())
                for key in keys_to_flatten:

                    def key_with_nan(x):
                        try:
                            return x[key]
                        except:
                            return x

                    df[column_flattened + "_" +
                        key] = df[column_flattened].apply(key_with_nan)
                    added_columns.append(column_flattened + "_" + key)
                    if debug:
                        print("Created column : " +
                              column_flattened + "_" + key)
                if drop:
                    df.drop(column_flattened, axis=1, inplace=True)
                    if debug:
                        print("Removed original " + column_flattened)
            else:
                if debug:
                    print("Keys are not the same on all rows.")
                    print(keylists_counts)

        # Find out whether the value is a list
        # TODO check on whole column
        elif isinstance(df[column_flattened][0], list):
            if debug:
                print("Column " + column_flattened + " is a list.")
            # check if all of same size

            series_sizes = df[column_flattened].apply(length_with_nan)

            sizes_counts = series_sizes.value_counts()
            if debug:
                print("Counts of different sizes: ", len(sizes_counts))
            # if all of same size
            if len(sizes_counts) == 1:
                if debug:
                    print("All of same size: " + str(sizes_counts))
                # and if size is one: we can flatten list [x] -> x
                if series_sizes[0] == 1:
                    if debug:
                        print(
                            "All lists are from size one, so we can take flatten it.")
                    df[column_flattened] = df[
                        column_flattened].apply(first_with_nan)
                    added_columns.append(column_flattened)
                    if debug:
                        print("Replaced column " + column_flattened)
            else:
                if debug:
                    print(
                        "Size are not the same or >1 so we cannot flatten list, but we can count the number of elements.")
                df[column_flattened +
                    "_size"] = df[column_flattened].apply(length_with_nan)
                added_columns.append(column_flattened + "_size")
                if debug:
                    print("New column: " + column_flattened + "_size")
        else:
            if debug:
                print("Column " + column_flattened +
                      " is not a dictionnary nor a list!")
    return added_columns


def flatten_dataframe(df, drop=False, max_depth=3, debug=False):
    """
    Flatten all columns of a given dataframe, with a max_depth defined.
    """
    cols_to_flatten = df.columns
    cols_flattened = []
    k = 1
    while k <= max_depth:
        if debug:
            print("-" * 30)
            print("FLATENNING LEVEL " + str(k))
            print("-" * 30)
        # we use new columns to flatten them
        cols_to_flatten = flatten_columns(df, cols_to_flatten, drop)
        cols_flattened.append(cols_to_flatten)
        k += 1
        if len(cols_to_flatten) == 0:
            if debug:
                print("-" * 30)
                print("END NO MORE COLUMNS TO FLATTEN")
                print("-" * 30)
            break
    return cols_flattened


class RequestParser:

    def __init__(self, request_results, asked_path):
        self.asked_path = asked_path
        self.results = request_results
        self.item_name = os.path.basename(asked_path)
        self.parsed = {}  # dictionary of page : dictionary
        self.parsing_errors = {}
        self.nested_items = None  # will be a dict
        self.unnested_items = None  # will be a dict
        self.links = []  # first page is enough
        self.disruptions = {}  # all pages
        self.keys = []  # collect keys found in request answer
        self.nbr_expected_items = None
        self.nbr_collected_items = None
        self.log = None

    def set_results(self, request_results):
        self.results = request_results

    def parse(self):
        self.parse_requests()
        self.extract_keys()
        self.extract_links()
        self.extract_disruptions()
        self.get_nested_items()
        self.get_unnested_items()
        self.extract_nbr_expected_items()
        self.count_nbr_collected_items()
        self.parse_log()

    def parse_requests(self):
        # First operation, to parse requests text into python dictionnaries
        for page, value in self.results.items():
            # Only add if answer was good
            try:
                if value.status_code == 200:
                    self.parsed[page] = json.loads(value.text)
            except ValueError:
                print("JSON decoding error.")
                self.parsing_errors[page] = "JSON decoding error"

    def get_nested_items(self):
        """
        Result is a dictionary, of one key: item_name, and value is list of items (concatenate all result pages).
        """
        dictionnary = {self.item_name: []}
        for page, value in self.parsed.items():
            # concatenate two lists of items
            dictionnary[self.item_name] += value[self.item_name]
        self.nested_items = dictionnary

    def get_nested_disruptions(self):
        """
        Result is a dictionary, of one key: item_name, and value is list of items (concatenate all result pages).
        """
        if self.item_name == "disruptions":
            return False

        dictionnary = {"disruptions": []}
        for page, value in self.parsed.items():
            # concatenate two lists of items
            dictionnary["disruptions"] += value["disruptions"]
        self.disruptions = dictionnary

    def get_unnested_items(self):
        df = pd.DataFrame(self.nested_items[self.item_name])
        flatten_dataframe(df, drop=True, max_depth=5)
        self.unnested_items = df.to_dict()

    def extract_keys(self):
        # Extract keys of first page
        try:
            self.keys = list(self.parsed[0].keys())
        except:
            pass

    def extract_links(self):
        # Extract from first page
        try:
            self.links = self.parsed[0]["links"]
        except KeyError:
            self.links = {"links": "Not found"}

    def extract_disruptions(self):
        # TODO extract all pages
        # Extract from first page
        try:
            self.disruptions = self.parsed[0]["disruptions"]
        except KeyError:
            self.disruptions = {"disruptions": "Not found"}

    def extract_nbr_expected_items(self):
        if self.results[0].status_code != 200:
            return None
        # Parse first request answer.
        parsed = json.loads(self.results[0].text)
        # Extract pagination part.
        pagination = parsed["pagination"]
        # Extract total_result
        self.nbr_expected_items = pagination["total_result"]

    def count_nbr_collected_items(self):
        unnested = pd.DataFrame(self.unnested_items)  # df
        self.nbr_collected_items = len(unnested.index)

    def explain(self):
        print("Parsing:")
        print("Keys found: " + str(self.keys))
        print(self.item_name.capitalize() + " has " +
              str(self.nbr_expected_items) + " elements.")

    def parse_log(self):
        log = {}
        log["number_requests"] = len(self.results)
        log["number_parsed"] = len(self.parsed)
        log["keys"] = self.keys
        log["nbr_announced_items"] = self.nbr_expected_items
        log["nbr_collected_items"] = self.nbr_collected_items
        log["item_columns"] = list(pd.DataFrame(
            self.unnested_items).columns.values)
        self.log = log
        log["parsing_errors"] = self.parsing_errors

    def write_all(self, directory):
        # Get results
        unnested = pd.DataFrame(self.unnested_items)  # df
        nested = self.nested_items  # dict
        # Write item csv
        unnested.to_csv(os.path.join(directory, self.item_name + ".csv"))
        # Write item json
        with open(os.path.join(directory, self.item_name + ".json"), 'w') as f:
            json.dump(nested, f, ensure_ascii=False)
        # Write links (of first page)
        with open(os.path.join(directory, "links.json"), 'w') as f:
            json.dump(self.links, f, ensure_ascii=False)
        # Write disruptions (if item different)
        if self.item_name != "disruptions":
            unnested_dis = pd.DataFrame(self.disruptions)  # df
            unnested_dis.to_csv(os.path.join(directory, "disruptions.csv"))
        # Write logs
        with open(os.path.join(directory, "parse_log.json"), 'w') as f:
            json.dump(self.log, f, ensure_ascii=False)

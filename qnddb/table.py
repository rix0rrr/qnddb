import base64
import json
from dataclasses import dataclass
from typing import Optional

from .storage.table_storage import TableStorage
from .result_page import ResultPage
from ._private.backoff import ExponentialBackoff
from .cursor import GetManyCursor
from . import querylog


class Index:
    """A DynamoDB index.

    The name of the index will be assumed to be '{partition_key}-{sort_key}-index', if not given.

    Specify if the index is a keys-only index. If not, is is expected to have all fields.
    """

    def __init__(self, partition_key: str, sort_key: str = None, index_name: str = None, keys_only: bool = False):
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.index_name = index_name
        self.keys_only = keys_only
        if not self.index_name:
            self.index_name = '-'.join([partition_key] + ([sort_key] if sort_key else [])) + '-index'


class Table:
    """Dynamo table access

    Transparently handles indexes, and doesn't support sort keys yet.

    Parameters:
        - partition_key: the partition key for the table.
        - indexes: a list of fields that have a GSI on them.
          Each individual index must be named '{field}-index', and each must
          project the full set of attributes. Indexes can have a partition and their
          own sort keys.
        - sort_key: a field that is the sort key for the table.
    """

    def __init__(self, storage: TableStorage, table_name, partition_key, sort_key=None, indexes=None):
        self.storage = storage
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.indexes = indexes or []
        self.key_names = [self.partition_key] + ([self.sort_key] if self.sort_key else [])

    @querylog.timed_as("db_get")
    def get(self, key):
        """Gets an item by key from the database.

        The key must be a dict with a single entry which references the
        partition key or an index key.
        """
        querylog.log_counter(f"db_get:{self.table_name}")
        lookup = self._determine_lookup(key, many=False)
        if isinstance(lookup, TableLookup):
            return self.storage.get_item(lookup.table_name, lookup.key)
        if isinstance(lookup, IndexLookup):
            return first_or_none(
                self.storage.query_index(
                    lookup.table_name, lookup.index_name, lookup.key, sort_key=lookup.sort_key, limit=1,
                    keys_only=lookup.keys_only, table_key_names=self.key_names,
                )[0]
            )
        assert False

    @querylog.timed_as("db_batch_get")
    def batch_get(self, keys):
        """Return a number of items by (primary+sort) key from the database.

        Keys can be either:
            - A dictionary, mapping some identifier to a database (dictionary) key
            - A list of database keys

        Depending on the input, returns either a dictionary with the same keys,
        or a list in the same order as the input list.

        Each key must be a dict with a single entry which references the
        partition key. This is currently not supporting index lookups.
        """
        querylog.log_counter(f"db_batch_get:{self.table_name}")
        input_is_dict = isinstance(keys, dict)

        keys_dict = keys if input_is_dict else {f'k{i}': k for i, k in enumerate(keys)}

        lookups = {k: self._determine_lookup(key, many=False) for k, key in keys_dict.items()}
        if any(not isinstance(lookup, TableLookup) for lookup in lookups.values()):
            raise RuntimeError(f'batch_get must query table, not indexes, in: {keys}')
        if not lookups:
            return {} if input_is_dict else []
        first_lookup = next(iter(lookups.values()))

        resp_dict = self.storage.batch_get_item(
            first_lookup.table_name, {k: l.key for k, l in lookups.items()}, table_key_names=self.key_names)
        if input_is_dict:
            return {k: resp_dict.get(k) for k in keys.keys()}
        else:
            return [resp_dict.get(f'k{i}') for i in range(len(keys))]

    @querylog.timed_as("db_get_many")
    def get_many(self, key, reverse=False, limit=None, pagination_token=None):
        """Gets a list of items by key from the database.

        The key must be a dict with a single entry which references the
        partition key or an index key.

        `get_many` reads up to 1MB of data from the database, or a maximum of `limit`
        records, whichever one is hit first.
        """
        querylog.log_counter(f"db_get_many:{self.table_name}")

        lookup = self._determine_lookup(key, many=True)
        if isinstance(lookup, TableLookup):
            items, next_page_token = self.storage.query(
                lookup.table_name,
                lookup.key,
                sort_key=self.sort_key,
                reverse=reverse,
                limit=limit,
                pagination_token=decode_page_token(pagination_token),
            )
        elif isinstance(lookup, IndexLookup):
            items, next_page_token = self.storage.query_index(
                lookup.table_name,
                lookup.index_name,
                lookup.key,
                sort_key=lookup.sort_key,
                reverse=reverse,
                limit=limit,
                pagination_token=decode_page_token(pagination_token),
                keys_only=lookup.keys_only,
                table_key_names=self.key_names,
            )
        else:
            assert False
        querylog.log_counter("db_get_many_items", len(items))
        return ResultPage(items, encode_page_token(next_page_token))

    def get_all(self, key, reverse=False):
        """Return an iterator that will iterate over all elements in the table matching the query.

        Iterating over all elements can take a long time, make sure you have a timeout in the loop
        somewhere!"""
        return GetManyCursor(self, key, reverse=reverse)

    @querylog.timed_as("db_create")
    def create(self, data):
        """Put a single complete record into the database."""
        if self.partition_key not in data:
            raise ValueError(f"Expecting '{self.partition_key}' field in create() call, got: {data}")
        if self.sort_key and self.sort_key not in data:
            raise ValueError(f"Expecting '{self.sort_key}' field in create() call, got: {data}")

        querylog.log_counter(f"db_create:{self.table_name}")
        self.storage.put(self.table_name, self._extract_key(data), data)
        return data

    def put(self, data):
        """An alias for 'create', if calling create reads uncomfortably."""
        return self.create(data)

    @querylog.timed_as("db_update")
    def update(self, key, updates):
        """Update select fields of a given record.

        The values of data can be plain data, or an instance of
        one of the subclasses of DynamoUpdate which represent
        updates that aren't representable as plain values.
        """
        querylog.log_counter(f"db_update:{self.table_name}")
        self._validate_key(key)

        return self.storage.update(self.table_name, key, updates)

    @querylog.timed_as("db_del")
    def delete(self, key):
        """Delete an item by primary key.

        Returns the delete item.
        """
        querylog.log_counter("db_del:" + self.table_name)
        self._validate_key(key)

        return self.storage.delete(self.table_name, key)

    @querylog.timed_as("db_del_many")
    def del_many(self, key):
        """Delete all items matching a key.

        DynamoDB does not support this operation natively, so we have to turn
        it into a fetch+batch delete (and since our N is small, we do
        repeated "single" deletes instead of doing a proper batch delete).
        """
        querylog.log_counter("db_del_many:" + self.table_name)

        # The result of get_many is paged, so we might need to do this more than once.
        to_delete = self.get_many(key)
        backoff = ExponentialBackoff()
        while to_delete:
            for item in to_delete:
                key = self._extract_key(item)
                self.delete(key)
            to_delete = self.get_many(key)
            backoff.sleep_when(to_delete)

    @querylog.timed_as("db_scan")
    def scan(self, limit=None, pagination_token=None):
        """Reads the entire table into memory."""
        querylog.log_counter("db_scan:" + self.table_name)
        items, next_page_token = self.storage.scan(
            self.table_name, limit=limit, pagination_token=decode_page_token(pagination_token)
        )
        return ResultPage(items, encode_page_token(next_page_token))

    @querylog.timed_as("db_describe")
    def item_count(self):
        querylog.log_counter("db_describe:" + self.table_name)
        return self.storage.item_count(self.table_name)

    def _determine_lookup(self, key_data, many):
        if any(not v for v in key_data.values()):
            raise ValueError(f"Key data cannot have empty values: {key_data}")

        keys = set(key_data.keys())
        table_keys = self._key_names()
        one_key = list(keys)[0]

        # We do a regular table lookup if both the table partition and sort keys occur in the given key.
        if keys == table_keys:
            return TableLookup(self.table_name, key_data)

        # We do an index table lookup if the partition (and possibly the sort key) of an index occur in the given key.
        for index in self.indexes:
            index_key_names = [x for x in [index.partition_key, index.sort_key] if x is not None]
            if keys == set(index_key_names) or one_key == index.partition_key:
                return IndexLookup(self.table_name, index.index_name, key_data,
                                   index.sort_key, keys_only=index.keys_only)

        if len(keys) != 1:
            raise RuntimeError(f"Getting key data: {key_data}, but expecting: {table_keys}")

        # If the one key matches the partition key, it must be because we also have a
        # sort key, but that's allowed because we are looking for 'many' records.
        if one_key == self.partition_key:
            if not many:
                raise RuntimeError(f"Looking up one value, but missing sort key: {self.sort_key} in {key_data}")
            return TableLookup(self.table_name, key_data)

        raise RuntimeError(f"Field not partition key or index: {one_key}")

    def _extract_key(self, data):
        """
        Extract the key data out of plain data.
        """
        if self.partition_key not in data:
            raise RuntimeError(f"Partition key '{self.partition_key}' missing from data: {data}")
        if self.sort_key and self.sort_key not in data:
            raise RuntimeError(f"Sort key '{self.sort_key}' missing from data: {data}")

        return {k: data[k] for k in self._key_names()}

    def _key_names(self):
        return set(x for x in [self.partition_key, self.sort_key] if x is not None)

    def _validate_key(self, key):
        if key.keys() != self._key_names():
            raise RuntimeError(f"key fields incorrect: {key} != {self._key_names()}")
        if any(not v for v in key.values()):
            raise RuntimeError(f"key fields cannot be empty: {key}")


@dataclass
class TableLookup:
    table_name: str
    key: dict


@dataclass
class IndexLookup:
    table_name: str
    index_name: str
    key: dict
    sort_key: Optional[str]
    keys_only: bool


def first_or_none(xs):
    return xs[0] if xs else None


def encode_page_token(x):
    """Encode a compound key page token (dict) to a string."""
    if x is None:
        return None
    return base64.urlsafe_b64encode(json.dumps(x).encode("utf-8")).decode("ascii")


def decode_page_token(x):
    """Decode string page token to compound key (dict)."""
    if x is None:
        return None
    return json.loads(base64.urlsafe_b64decode(x.encode("ascii")).decode("utf-8"))

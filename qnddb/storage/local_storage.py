import copy
import json
import logging

from .table_storage import TableStorage
from ._synchronized import Lock
from ._helpers import validate_only_sort_key
from ..conditions import DynamoCondition
from ..updates import DynamoUpdate

logger = logging.getLogger(__name__)

lock = Lock()

class LocalStorage(TableStorage):
    def __init__(self, filename=None):
        # In-memory structure:
        #
        # { table_name -> [ {...record...}, {...record...} ] }
        #
        # Needs work done to support sort keys properly
        self.tables = {}
        self.filename = filename

        if filename:
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    self.tables = json.load(f, object_hook=CustomEncoder.decode_object)
            except IOError:
                pass
            except json.decoder.JSONDecodeError as e:
                logger.warning(
                    f"Error loading {filename}. The next write operation \
                        will overwrite the database with a clean copy: {e}"
                )

    # NOTE: on purpose not @synchronized here
    def get_item(self, table_name, key):
        items, _ = self.query(table_name, key, sort_key=None, reverse=False, limit=None, pagination_token=None)
        return first_or_none(items)

    def batch_get_item(self, table_name, keys_map, table_key_names):
        # The in-memory implementation is lovely and trivial
        return {k: self.get_item(table_name, key) for k, key in keys_map.items()}

    @lock.synchronized
    def query(self, table_name, key, sort_key, reverse, limit, pagination_token):
        eq_conditions, special_conditions = DynamoCondition.partition(key)
        validate_only_sort_key(special_conditions, sort_key)

        records = self.tables.get(table_name, [])
        filtered = [r for r in records if self._query_matches(r, eq_conditions, special_conditions)]

        if sort_key:
            filtered.sort(key=lambda x: x[sort_key])

        if reverse:
            filtered.reverse()

        # Pagination token
        def extract_key(i, record):
            ret = {k: record[k] for k in key.keys()}
            if sort_key is None:
                ret["offset"] = i
            else:
                ret[sort_key] = record[sort_key]
            return ret

        def orderable(key):
            partition_key = [k for k in key.keys() if k != sort_key][0]
            second_key = [k for k in key.keys() if k != partition_key][0]
            return (key[partition_key], key[second_key])

        def before_or_equal(key0, key1):
            k0 = orderable(key0)
            k1 = orderable(key1)
            return k0 <= k1 if not reverse or not sort_key else k1 <= k0

        with_keys = [(extract_key(i, r), r) for i, r in enumerate(filtered)]
        while pagination_token and with_keys and before_or_equal(with_keys[0][0], pagination_token):
            with_keys.pop(0)

        next_page_key = None
        if limit and limit < len(with_keys):
            with_keys = with_keys[:limit]
            next_page_key = with_keys[-1][0]

        return copy.copy([record for _, record in with_keys]), next_page_key

    # NOTE: on purpose not @synchronized here
    def query_index(self, table_name, index_name, keys, sort_key, reverse=False, limit=None, pagination_token=None,
                    keys_only=None, table_key_names=None):
        # If keys_only, we project down to the index + table keys
        # In a REAL dynamo table, the index just wouldn't have more data. The in-memory table has everything,
        # so we need to drop some data so programmers don't accidentally rely on it.

        records, next_page_token = self.query(
            table_name, keys, sort_key=sort_key, reverse=reverse, limit=limit, pagination_token=pagination_token
        )

        if not keys_only:
            return records, next_page_token

        # In a keys_only index, we retain all fields that are in either a table or index key
        keys_to_retain = set(list(keys.keys()) + ([sort_key] if sort_key else []) + table_key_names)
        return [{key: record[key] for key in keys_to_retain} for record in records], next_page_token

    @lock.synchronized
    def put(self, table_name, key, data):
        records = self.tables.setdefault(table_name, [])
        index = self._find_index(records, key)
        if index is None:
            records.append(copy.copy(data))
        else:
            records[index] = copy.copy(data)
        self._flush()

    @lock.synchronized
    def update(self, table_name, key, updates):
        records = self.tables.setdefault(table_name, [])
        index = self._find_index(records, key)
        if index is None:
            records.append(key.copy())
            index = len(records) - 1

        record = records[index]
        for name, update in updates.items():
            if isinstance(update, DynamoUpdate):
                update.apply_in_memory(record, name)
            elif update is None:
                if name in record:
                    del record[name]
            else:
                # Plain value update
                record[name] = update

        self._flush()
        return record.copy()

    @lock.synchronized
    def delete(self, table_name, key):
        records = self.tables.get(table_name, [])
        index = self._find_index(records, key)
        ret = None
        if index is not None:
            ret = records.pop(index)
            self._flush()
        return ret

    @lock.synchronized
    def item_count(self, table_name):
        return len(self.tables.get(table_name, []))

    @lock.synchronized
    def scan(self, table_name, limit, pagination_token):
        items = self.tables.get(table_name, [])[:]

        start_index = 0
        if pagination_token:
            start_index = pagination_token["offset"]
            items = items[pagination_token["offset"]:]

        next_page_token = None
        if limit and limit < len(items):
            next_page_token = {"offset": start_index + limit}
            items = items[:limit]

        items = copy.copy(items)
        return items, next_page_token

    def _find_index(self, records, key):
        for i, v in enumerate(records):
            if self._eq_matches(v, key):
                return i
        return None

    def _eq_matches(self, record, key):
        return all(record.get(k) == v for k, v in key.items())

    def _query_matches(self, record, eq, conds):
        return all(record.get(k) == v for k, v in eq.items()) and all(
            cond.matches(record.get(k)) for k, cond in conds.items()
        )

    def _flush(self):
        if self.filename:
            try:
                with open(self.filename, "w", encoding="utf-8") as f:
                    json.dump(self.tables, f, indent=2, cls=CustomEncoder)
            except IOError:
                pass


def first_or_none(xs):
    return xs[0] if xs else None


class CustomEncoder(json.JSONEncoder):
    """An encoder that serializes non-standard types like sets."""

    def default(self, obj):
        if isinstance(obj, set):
            return {"$type": "set", "elements": list(obj)}
        return json.JSONEncoder.default(self, obj)

    @staticmethod
    def decode_object(obj):
        """The decoding for the encoding above."""
        if obj.get("$type") == "set":
            return set(obj["elements"])
        return obj

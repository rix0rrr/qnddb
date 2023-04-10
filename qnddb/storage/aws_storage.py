import os
import collections

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from .table_storage import TableStorage
from ._helpers import validate_only_sort_key
from .._private.backoff import ExponentialBackoff
from ..conditions import DynamoCondition
from ..updates import DynamoUpdate

class AwsDynamoStorage(TableStorage):
    @staticmethod
    def from_env():
        # If we have AWS credentials, use the real DynamoDB
        if os.getenv("AWS_ACCESS_KEY_ID"):
            db = boto3.client("dynamodb", region_name=config["dynamodb"]["region"])
            db_prefix = os.getenv("AWS_DYNAMODB_TABLE_PREFIX", "")
            return AwsDynamoStorage(db, db_prefix)
        return None

    def __init__(self, db, db_prefix):
        self.db = db
        self.db_prefix = db_prefix

    def get_item(self, table_name, key):
        result = self.db.get_item(TableName=make_table_name(self.db_prefix, table_name), Key=self._encode(key))
        return self._decode(result.get("Item", None))

    def batch_get_item(self, table_name, keys_map, table_key_names):
        # Do a batch query to DynamoDB. Handle that DDB will do at most 100 items by chunking.
        real_table_name = make_table_name(self.db_prefix, table_name)

        def immutable_key(record):
            # Extract the key fields from the record and make them suitable for indexing a dict
            return frozenset({k: record[k] for k in table_key_names}.items())

        # The input may have duplicates, but DynamoDB no likey
        key_to_ids = collections.defaultdict(list)
        to_query = []
        for id, key in keys_map.items():
            imkey = immutable_key(key)
            if imkey not in key_to_ids:
                to_query.append(self._encode(key))
            key_to_ids[imkey].append(id)

        ret = {}
        next_query = []

        def fill_er_up():
            # Fill up next_query (from to_query) until it has at most 100 items
            to_eat = min(100 - len(next_query), len(to_query))
            next_query.extend(to_query[:to_eat])
            to_query[:to_eat] = []

        fill_er_up()
        backoff = ExponentialBackoff()
        while next_query:
            result = self.db.batch_get_item(
                RequestItems={real_table_name: {'Keys': next_query}}
            )
            for row in result.get('Responses', {}).get(real_table_name, []):
                record = self._decode(row)
                for id in key_to_ids[immutable_key(record)]:
                    ret[id] = record

            # The DB may not have done everything (we might have gotten throttled). If so, sleep.
            next_query = result.get('UnprocessedKeys', {}).get(real_table_name, {}).get('Keys', [])
            backoff.sleep_when(to_query)
            fill_er_up()

        return ret

    def query(self, table_name, key, sort_key, reverse, limit, pagination_token):
        key_expression, attr_values, attr_names = self._prep_query_data(key, sort_key)
        result = self.db.query(
            **notnone(
                TableName=make_table_name(self.db_prefix, table_name),
                KeyConditionExpression=key_expression,
                ExpressionAttributeValues=attr_values,
                ScanIndexForward=not reverse,
                ExpressionAttributeNames=attr_names,
                Limit=limit,
                ExclusiveStartKey=self._encode(pagination_token) if pagination_token else None,
            )
        )

        items = [self._decode(x) for x in result.get("Items", [])]
        next_page_token = self._decode(result.get("LastEvaluatedKey", None))
        return items, next_page_token

    def query_index(self, table_name, index_name, keys, sort_key, reverse=False, limit=None, pagination_token=None,
                    keys_only=None, table_key_names=None):
        # keys_only is ignored here -- that's only necessary for the in-memory implementation.
        # In an actual DDB table, that's an attribute of the index itself

        key_expression, attr_values, attr_names = self._prep_query_data(keys, sort_key)

        result = self.db.query(
            **notnone(
                TableName=make_table_name(self.db_prefix, table_name),
                IndexName=index_name,
                KeyConditionExpression=key_expression,
                ExpressionAttributeValues=attr_values,
                ScanIndexForward=not reverse,
                ExpressionAttributeNames=attr_names,
                Limit=limit,
                ExclusiveStartKey=self._encode(pagination_token) if pagination_token else None,
            )
        )

        items = [self._decode(x) for x in result.get("Items", [])]
        next_page_token = (
            self._decode(result.get("LastEvaluatedKey", None)) if result.get("LastEvaluatedKey", None) else None
        )
        return items, next_page_token

    def _prep_query_data(self, key, sort_key=None):
        eq_conditions, special_conditions = DynamoCondition.partition(key)
        validate_only_sort_key(special_conditions, sort_key)

        # We must escape field names with a '#' because Dynamo is unhappy
        # with fields called 'level' etc:
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
        # This escapes too much, but at least it's easy.

        key_expression = " AND ".join(
            [f"#{field} = :{field}" for field in eq_conditions.keys()]
            + [cond.to_dynamo_expression(field) for field, cond in special_conditions.items()]
        )

        attr_values = {f":{field}": DDB_SERIALIZER.serialize(key[field]) for field in eq_conditions.keys()}
        for field, cond in special_conditions.items():
            attr_values.update(cond.to_dynamo_values(field))

        attr_names = {"#" + field: field for field in key.keys()}

        return key_expression, attr_values, attr_names

    def put(self, table_name, _key, data):
        self.db.put_item(TableName=make_table_name(self.db_prefix, table_name), Item=self._encode(data))

    def update(self, table_name, key, updates):
        value_updates = {k: v for k, v in updates.items() if not isinstance(v, DynamoUpdate)}
        special_updates = {k: v.to_dynamo() for k, v in updates.items() if isinstance(v, DynamoUpdate)}

        response = self.db.update_item(
            TableName=make_table_name(self.db_prefix, table_name),
            Key=self._encode(key),
            AttributeUpdates={
                **self._encode_updates(value_updates),
                **special_updates,
            },
            # Return the full new item after update
            ReturnValues='ALL_NEW',
        )
        return self._decode(response.get('Attributes', {}))

    def delete(self, table_name, key):
        return self.db.delete_item(TableName=make_table_name(self.db_prefix, table_name), Key=self._encode(key))

    def item_count(self, table_name):
        result = self.db.describe_table(TableName=make_table_name(self.db_prefix, table_name))
        return result["Table"]["ItemCount"]

    def scan(self, table_name, limit, pagination_token):
        result = self.db.scan(
            **notnone(
                TableName=make_table_name(self.db_prefix, table_name),
                Limit=limit,
                ExclusiveStartKey=self._encode(pagination_token) if pagination_token else None,
            )
        )
        items = [self._decode(x) for x in result.get("Items", [])]
        next_page_token = (
            self._decode(result.get("LastEvaluatedKey", None)) if result.get("LastEvaluatedKey", None) else None
        )
        return items, next_page_token

    def _encode(self, data):
        return {k: DDB_SERIALIZER.serialize(v) for k, v in data.items()}

    def _encode_updates(self, data):
        def encode_update(value):
            # None is special, we use it to remove a field from a record
            if value is None:
                return {"Action": "DELETE"}
            else:
                return {"Value": DDB_SERIALIZER.serialize(value)}

        return {k: encode_update(v) for k, v in data.items()}

    def _decode(self, data):
        if data is None:
            return None

        return {k: replace_decimals(DDB_DESERIALIZER.deserialize(v)) for k, v in data.items()}


def make_table_name(prefix, name):
    return f"{prefix}-{name}" if prefix else name

DDB_SERIALIZER = TypeSerializer()
DDB_DESERIALIZER = TypeDeserializer()


def notnone(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not None}


def replace_decimals(obj):
    """
    Replace Decimals with native Python values.

    The default DynamoDB deserializer returns Decimals instead of ints,
    which we can't to-JSON. *sigh*.
    """
    import decimal

    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj
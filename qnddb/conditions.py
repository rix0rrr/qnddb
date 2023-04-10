from boto3.dynamodb.types import TypeSerializer
DDB_SERIALIZER = TypeSerializer()

class DynamoCondition:
    """Base class for Query conditions.

    These encode any type of comparison supported by Dynamo except equality.

    Conditions only apply to sort keys.
    """

    def to_dynamo_expression(self, _field_name):
        """Render expression part of Dynamo query."""
        raise NotImplementedError()

    def to_dynamo_values(self, _field_name):
        """Render values for the Dynamo expression."""
        raise NotImplementedError()

    def matches(self, value):
        """Whether or not the given value matches the condition (for in-memory db)."""
        raise NotImplementedError()

    @staticmethod
    def partition(key):
        """Partition a dictionary into 2 dictionaries.

        The first one will contain all elements for which the values are
        NOT of type DynamoCondition. The other one will contain all the elements
        for which the value ARE DynamoConditions.
        """
        eq_conditions = {k: v for k, v in key.items() if not isinstance(v, DynamoCondition)}
        special_conditions = {k: v for k, v in key.items() if isinstance(v, DynamoCondition)}

        return (eq_conditions, special_conditions)


class Between(DynamoCondition):
    """Assert that a value is between two other values."""

    def __init__(self, minval, maxval):
        self.minval = minval
        self.maxval = maxval

    def to_dynamo_expression(self, field_name):
        return f"#{field_name} BETWEEN :{field_name}_min AND :{field_name}_max"

    def to_dynamo_values(self, field_name):
        return {
            f":{field_name}_min": DDB_SERIALIZER.serialize(self.minval),
            f":{field_name}_max": DDB_SERIALIZER.serialize(self.maxval),
        }

    def matches(self, value):
        return self.minval <= value <= self.maxval

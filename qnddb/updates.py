import numbers

from boto3.dynamodb.types import TypeSerializer
DDB_SERIALIZER = TypeSerializer()


class DynamoUpdate:
    def to_dynamo(self):
        raise NotImplementedError()

    def apply_in_memory(self, record, name):
        raise NotImplementedError()


class DynamoIncrement(DynamoUpdate):
    def __init__(self, delta=1):
        self.delta = delta

    def to_dynamo(self):
        return {
            "Action": "ADD",
            "Value": {"N": str(self.delta)},
        }

    def apply_in_memory(self, record, name):
        record[name] = record.get(name, 0) + self.delta


class DynamoAddToStringSet(DynamoUpdate):
    """Add one or more elements to a string set."""

    def __init__(self, *elements):
        self.elements = elements

    def to_dynamo(self):
        return {
            "Action": "ADD",
            "Value": {"SS": list(self.elements)},
        }

    def apply_in_memory(self, record, name):
        existing = record.get(name, set())
        if not isinstance(existing, set):
            raise TypeError(f"Expected a set in {name}, got: {existing}")
        record[name] = existing | set(self.elements)


class DynamoAddToNumberSet(DynamoUpdate):
    """Add one or more elements to a number set."""

    def __init__(self, *elements):
        for el in elements:
            if not isinstance(el, numbers.Real):
                raise ValueError(f"Must be a number, got: {el}")
        self.elements = elements

    def to_dynamo(self):
        return {
            "Action": "ADD",
            "Value": {"NS": [str(x) for x in self.elements]},
        }

    def apply_in_memory(self, record, name):
        existing = record.get(name, set())
        if not isinstance(existing, set):
            raise TypeError(f"Expected a set in {name}, got: {existing}")
        record[name] = existing | set(self.elements)


class DynamoAddToList(DynamoUpdate):
    """Add one or more elements to a list."""

    def __init__(self, *elements):
        self.elements = elements

    def to_dynamo(self):
        return {
            "Action": "ADD",
            "Value": {"L": [DDB_SERIALIZER.serialize(x) for x in self.elements]},
        }

    def apply_in_memory(self, record, name):
        existing = record.get(name, [])
        if not isinstance(existing, list):
            raise TypeError(f"Expected a list in {name}, got: {existing}")
        record[name] = existing + list(self.elements)


class DynamoRemoveFromStringSet(DynamoUpdate):
    """Remove one or more elements to a string set."""

    def __init__(self, *elements):
        self.elements = elements

    def to_dynamo(self):
        return {
            "Action": "DELETE",
            "Value": {"SS": list(self.elements)},
        }

    def apply_in_memory(self, record, name):
        existing = record.get(name, set())
        if not isinstance(existing, set):
            raise TypeError(f"Expected a set in {name}, got: {existing}")
        record[name] = existing - set(self.elements)

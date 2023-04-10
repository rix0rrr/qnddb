from abc import ABCMeta

class TableStorage(metaclass=ABCMeta):
    def get_item(self, table_name, key):
        ...

    def batch_get_item(self, table_name, keys_map, table_key_names):
        ...

    # The 'sort_key' argument for query and query_index is used to indicate that one of the keys is a sort_key
    # This is now needed because we can query by index sort key too. Still hacky hacky :).
    def query(self, table_name, key, sort_key, reverse, limit, pagination_token):
        ...

    def query_index(self, table_name, index_name, keys, sort_key, reverse=False,
                    limit=None, pagination_token=None, keys_only=None, table_key_names=None):
        ...

    def put(self, table_name, key, data):
        """Put the given data under the given key.

        Does not need to return anything.
        """
        ...

    def update(self, table_name, key, updates):
        """Update the given record, identified by a key, with updates.

        Must return the updated state of the record.
        """
        ...

    def delete(self, table_name, key):
        ...

    def item_count(self, table_name):
        ...

    def scan(self, table_name, limit, pagination_token):
        ...



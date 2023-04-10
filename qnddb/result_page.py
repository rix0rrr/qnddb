from dataclasses import dataclass
from typing import List, Optional

@dataclass
class ResultPage:
    """A page of results, as returned by get_many().

    If the field `next_page_token` has a non-`None` value,
    there is more data to retrieve.

    Implements the iterator protocol, so can be used in a `for`
    loop. To convert it to a `list`, write:

        result_list = list(table.get_many(...))
    """

    records: List[dict]
    next_page_token: Optional[str]

    @property
    def has_next_page(self):
        return bool(self.next_page_token)

    def __iter__(self):
        return iter(self.records)

    def __getitem__(self, i):
        return self.records[i]

    def __len__(self):
        return len(self.records)

    def __nonzero__(self):
        return bool(self.records)

    __bool__ = __nonzero__


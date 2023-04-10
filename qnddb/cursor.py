from .result_page import ResultPage

class QueryCursor:
    """Iterate over a set of query results, keeping track of a database and client side
    pagination token at the same time.

    _do_fetch() should be overridden, and use self.pagination_token to
    paginate on the database.
    """

    def __init__(self, pagination_token=None):
        drop_initial = self._analyze_pagination_token(pagination_token)
        self.page = None
        self.have_eof = None

        # Eat records according to the amount we needed to drop
        i = 0
        while not self.eof and i < drop_initial:
            i += 1
            self.advance()

    def _fetch_next_page(self):
        self.i = 0
        self.page = self._do_fetch()
        if not isinstance(self.page, ResultPage):
            raise RuntimeError('_do_fetch must return a ResultPage')

        self.have_eof = len(self.page) == 0

    def _do_fetch(self):
        raise NotImplementedError('_do_fetch should be implemented')

    @property
    def eof(self):
        if self.have_eof:
            # This removes the need for fetches just to answer the EOF question
            return self.have_eof

        if self.page is None:
            self._fetch_next_page()

        # If we have a page, we're at eof if we're at the end of the last page
        return self.i >= len(self.page) and not self.page.next_page_token

    def advance(self):
        if self.page is None:
            self._fetch_next_page()
            return

        self.i += 1
        if self.i >= len(self.page) and self.page.next_page_token:
            self.pagination_token = self.page.next_page_token
            if self.pagination_token is None:
                self.have_eof = True

            # Reset self.page, so we don't retrieve the next page unnecessarily
            # but the next lookup will fetch it
            self.page = None

    @property
    def current(self):
        if self.page is None:
            self._fetch_next_page()
        if self.eof:
            raise RuntimeError('At eof')
        return self.page[self.i]

    def __iter__(self):
        return PythonQueryIterator(self)

    def __nonzero__(self):
        return not self.eof

    __bool__ = __nonzero__

    def _analyze_pagination_token(self, x):
        """Turn a pagination token into a DB part and a client part.

        Return the number part.
        """
        if not x:
            self.pagination_token = None
            return 0

        parts = x.split('@')
        self.pagination_token = parts[0] or None
        return int(parts[1])

    @property
    def next_page_token(self):
        if self.eof:
            return None
        return f'{self.pagination_token or ""}@{self.i}'


class PythonQueryIterator:
    """Implements the Python iterator protocol, which is slightly different
    from the Java (eof/current/advance) iterator protocol.
    """

    def __init__(self, iter):
        self.iter = iter

    def __next__(self):
        if self.iter.eof:
            raise StopIteration()
        ret = self.iter.current
        self.iter.advance()
        return ret


class GetManyCursor(QueryCursor):
    """Iterate over a set of query results, automatically proceeding to the next result page if necessary.

    Wrapper around query_many that automatically paginates.
    """

    def __init__(self, table, key, reverse=False, limit=None, pagination_token=None):
        self.table = table
        self.key = key
        self.reverse = reverse
        self.limit = limit
        super().__init__(pagination_token)

    def _do_fetch(self):
        return self.table.get_many(self.key,
                                   reverse=self.reverse,
                                   limit=self.limit,
                                   pagination_token=self.pagination_token)


class ScanCursor(QueryCursor):
    """Iterate over a table scan, automatically proceeding to the next result page if necessary.

    Wrapper around scan that automatically paginates.
    """

    def __init__(self, table, limit=None, pagination_token=None):
        self.table = table
        self.limit = limit
        super().__init__(pagination_token)

    def _do_fetch(self):
        return self.table.scan(limit=self.limit, pagination_token=self.pagination_token)



class _Page:
    def __init__(self, values, has_next, next_after):
        self.has_next = has_next
        self.values = values
        self.next_after = next_after

    @staticmethod
    def empty():
        return _Page([], False, None)

    @staticmethod
    def initial(after):
        return _Page([], True, after)


class _PageIterator:
    def __init__(self, page: _Page, get_next_page):
        self.page = page
        self.get_next_page = get_next_page

    def __iter__(self):
        return self

    def __next__(self):
        if not self.page.values:
            if self.page.has_next:
                self.page = self.get_next_page(self.page)
            if not self.page.values:
                raise StopIteration
        return self.page.values.pop(0)


class _Paginated:
    def __init__(self, paginated_getter, pluck_page_resources_from_response):
        self.paginated_getter = paginated_getter
        self.pluck_page_resources_from_response = pluck_page_resources_from_response

    def find_iter(self, **kwargs):
        """Iterate over resources with pagination.

        :key str org: The organization name.
        :key str org_id: The organization ID.
        :key str after: The last resource ID from which to seek from (but not including).
        :key int limit: the maximum number of items per page
        :return: resources iterator
        """

        def get_next_page(page: _Page):
            return self._find_next_page(page, **kwargs)

        return iter(_PageIterator(_Page.initial(kwargs.get('after')), get_next_page))

    def _find_next_page(self, page: _Page, **kwargs):
        if not page.has_next:
            return _Page.empty()

        kw_args = {**kwargs, 'after': page.next_after} if page.next_after is not None else kwargs
        response = self.paginated_getter(**kw_args)

        resources = self.pluck_page_resources_from_response(response)
        has_next = response.links.next is not None
        last_id = resources[-1].id if resources else None

        return _Page(resources, has_next, last_id)

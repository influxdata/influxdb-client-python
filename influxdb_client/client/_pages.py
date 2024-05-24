

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

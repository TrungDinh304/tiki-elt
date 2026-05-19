from crawler.fetch_tiki import fetch_products, fetch_products_for_category

# Using pytest-mock or monkeypatch in reality to stub API calls.
# Here is a placeholder mock test structure.


def test_fetch_products_for_category_returns_list(monkeypatch):
    """Ensure per-category fetch returns a list object."""

    class MockResponse:
        status_code = 200

        def json(self):
            return {"data": [{"id": 1, "name": "Fake Product", "price": 100}]}

    def mock_get(*args, **kwargs):
        return MockResponse()

    from crawler import fetch_tiki

    monkeypatch.setattr(fetch_tiki.SESSION, "get", mock_get)

    products = fetch_products_for_category(8322, num_pages=1)
    assert isinstance(products, list)
    assert len(products) > 0


def test_fetch_products_accepts_category_list(monkeypatch):
    """Top-level fetch_products iterates a list of category ids."""

    class MockResponse:
        status_code = 200

        def json(self):
            return {"data": [{"id": 1, "name": "Fake Product", "price": 100}]}

    def mock_get(*args, **kwargs):
        return MockResponse()

    from crawler import fetch_tiki

    monkeypatch.setattr(fetch_tiki.SESSION, "get", mock_get)

    products = fetch_products([8322, 1789], num_pages=1)
    assert isinstance(products, list)
    assert len(products) >= 2

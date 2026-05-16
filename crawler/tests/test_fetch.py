from crawler.fetch_tiki import fetch_products

# Using pytest-mock or monkeypatch in reality to stub API calls.
# Here is a placeholder mock test structure.


def test_fetch_products_returns_list(monkeypatch):
    """Ensure our fetch function returns a list object."""

    class MockResponse:
        status_code = 200

        def json(self):
            return {"data": [{"id": 1, "name": "Fake Product", "price": 100}]}

    def mock_get(*args, **kwargs):
        return MockResponse()

    import requests

    monkeypatch.setattr(requests, "get", mock_get)

    products = fetch_products(num_pages=1)
    assert isinstance(products, list)
    assert len(products) > 0



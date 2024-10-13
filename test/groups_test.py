
from app.truth_service import TruthService

def test_groups():

    ts = TruthService()

    query = "dogs"
    results = ts.client.search_simpler(resource_type="groups", query=query, limit=100)
    assert len(results) == 20


from app.truth_service import TruthService, VERBOSE_MODE
from app.bq_service import BigQueryService, generate_timestamp


if __name__ == "__main__":

    ts = TruthService()
    bq = BigQueryService()

    breakpoint()

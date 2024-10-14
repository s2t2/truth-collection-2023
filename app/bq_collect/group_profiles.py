import os
from time import sleep
import datetime

from pandas import DataFrame

from app.truth_service import TruthService, VERBOSE_MODE
from app.bq_service import BigQueryService, generate_timestamp
from app.db import EXPORTS_DIR
from app.models.group_profile import GroupProfile


def fetch_groups_for_collection(bq, date=None):
    if date:
        date = date.replace(";","") # prevent sql injection for good measure
    else:
        date = datetime.date.today().strftime("%Y-%m-%d")

    sql = f"""
        SELECT gp.group_id, g.slug, g.display_name, date(max(gp.collected_at)) as last_collected_on
        FROM `{bq.dataset_address}.groups` g
        LEFT JOIN `{bq.dataset_address}.group_profiles` gp ON gp.slug = g.slug
        GROUP BY 1,2,3
        HAVING (last_collected_on IS NULL) OR (last_collected_on < '{date}')
    """
    return list(bq.execute_query(sql, verbose=False))


if __name__ == "__main__":

    ts = TruthService()
    bq = BigQueryService()

    groups = fetch_groups_for_collection(bq)

    records = []
    for group_info in groups:
        print("-------------")
        print(group_info)
        group_id = group_info["group_id"]
        slug = group_info["slug"]
        display_name = group_info["display_name"]

        try:
            # search likes the names, not the slugs
            query = display_name #.replace("#","") # some groups have # in the name, but search doesn't like #. JK # is fine?
            results = ts.client.search_simpler(resource_type="groups", query=query)
            # match results on display name:
            group_info = [g for g in results if g["display_name"].upper() == display_name.upper()][0]

            group = GroupProfile(group_info)

            records.append({
                "group_id": group.group_id,
                "owner_id": group.owner_id,
                "slug": group.slug,
                "created_at": group.created_at,
                "membership_required": group.membership_required,
                "visibility": group.visibility,
                "display_name": group.display_name,
                "members_count": group.members_count,
                "avatar_url": group.avatar_url,
                "header_url": group.header_url,
                "note_html": group.note_html,
                "tag_names": group.tag_names,
                "collected_at": generate_timestamp()
            })

        #except TypeError as err:
        #    print("OOPS NO DATA")
        #    print(results)
        #    continue
        except IndexError as err:
            print("OOPS, NO MATCHING GROUPS FOUND")
            breakpoint()
            continue
        except Exception as err:
            print("OOPS", type(err), err)
            #breakpoint()
            #sleep(3)
            continue
            #break # assume we are getting access restricted

    print("PROCESSED", len(records), "RECORDS")

    if any(records):

        print("SAVING TO CSV...")
        groups_df = DataFrame(records)
        print(groups_df.head())
        csv_filepath = os.path.join(EXPORTS_DIR, f"group_profiles_{datetime.date.today().strftime('%Y%m%d')}.csv")
        groups_df.to_csv(csv_filepath, mode="a")
        print(csv_filepath)

        print("SAVING TO BQ...")
        bq.insert_records_in_batches(bq.group_profiles_table, records)

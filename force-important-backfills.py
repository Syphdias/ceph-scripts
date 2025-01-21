#!/usr/bin/env python3
import json
import subprocess
from argparse import ArgumentParser
from sys import stderr


def osds_over(osd_df_data, min_utilization=85, count=3):
    """Get list of fullest osds
    Two limits can be used:
    min_utilization: only return above this utilization (0 for all)
    count: only the highest n (0 for all, -2 for excpet last 2)
    """
    over_osds = []
    for osd in osd_df_data["nodes"]:
        if osd["utilization"] > min_utilization:
            over_osds.append(osd)

    # convert 0 to None
    if not count:
        count = None

    return sorted(over_osds, key=lambda i: i["utilization"], reverse=True)[:count]


def main(args):
    osd_df_data = json.loads(
        subprocess.check_output("ceph osd df --format=json".split()).decode("utf-8")
    )

    important_osds = osds_over(
        osd_df_data, min_utilization=args.min_utilization, count=args.osd_count
    )
    important_osd_ids = [osd["id"] for osd in important_osds]

    pg_dump_data = json.loads(
        subprocess.check_output("ceph pg dump pgs --format=json".split()).decode(
            "utf-8"
        )
    )

    pg_count_limit = args.pg_count

    # compatibility for older ceph versions
    if "pg_stats" in pg_dump_data.keys():
        if not pg_dump_data.get("pg_ready"):
            print("Sorry, `pg_ready` not true in `ceph pg dump pgs`", file=stderr)
        pg_dump_data = pg_dump_data.get("pg_stats", [])

    for pg in pg_dump_data:
        for osd_id in important_osd_ids:
            if (
                osd_id in pg["acting"]
                and osd_id not in pg["up"]
                and "backfill" in pg["state"]
                and "forced_backfill" not in pg["state"]
            ):
                # - data is on important Ceph OSD (acting)
                # - data does not want to got ot important Ceph OSD (up)
                # - PG is backfilling but not yet forced to backfill

                if pg_count_limit <= 0:
                    break
                pg_count_limit -= 1

                if not args.quiet:
                    print(
                        "{:>6} on {:>3}: {:>15} -> {:>15} ({})".format(
                            pg["pgid"],
                            osd_id,
                            str(pg["acting"]),
                            str(pg["up"]),
                            pg["state"],
                        )
                    )
                if not args.dry:
                    subprocess.check_output(
                        ["ceph", "pg", "force-backfill", pg["pgid"]]
                    )


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "--min-utilization",
        type=int,
        default=85,
        help="Limit Ceph OSDs to be forced to at least this utilization",
    )
    parser.add_argument(
        "--osd-count",
        type=int,
        default=3,
        help="Limit of Ceph OSDs to the top n (default: 3)",
    )
    parser.add_argument(
        "--pg-count",
        type=int,
        default=20,
        help="Limit forcing to some pgs (default: 20)",
    )
    parser.add_argument(
        "--dry",
        action="store_true",
        default=False,
        help="Only print pgs to be forced",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Dont't print pgs to be forced",
    )

    args = parser.parse_args()
    main(args)

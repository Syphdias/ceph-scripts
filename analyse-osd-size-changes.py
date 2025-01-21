#!/usr/bin/env python3
from argparse import ArgumentParser
from subprocess import run
from json import loads
from sys import stderr


def json_from_file_or_command(file: str | None, command: str) -> dict:
    output_string = ""
    if file:
        with open(file, "r") as f:
            output_string = f.read()
    else:
        result = run(command.split(), capture_output=True, text=True, check=True)
        output_string = result.stdout
    return loads(output_string)


def main(args) -> None:
    osd_df = json_from_file_or_command(args.ceph_osd_df, "ceph osd df --format json")
    pg_dump = json_from_file_or_command(
        args.ceph_pg_dump_pgs, "ceph pg dump pgs --format json"
    )

    sizes = {}
    acting_sizes = {}

    # compatibility for older ceph versions
    if "pg_stats" in pg_dump.keys():
        if not pg_dump.get("pg_ready"):
            print("Sorry, `pg_ready` not true in `ceph pg dump pgs`", file=stderr)
            exit(1)
        pg_dump = pg_dump.get("pg_stats", [])

    for pg in pg_dump:
        for osd_up in pg["up"]:
            # if balanced
            if sizes.get(osd_up):
                sizes[osd_up] += pg["stat_sum"]["num_bytes"]
            else:
                sizes[osd_up] = pg["stat_sum"]["num_bytes"]
        for osd_acting in pg["acting"]:
            # if misplaced
            if acting_sizes.get(osd_acting):
                acting_sizes[osd_acting] += pg["stat_sum"]["num_bytes"]
            else:
                acting_sizes[osd_acting] = pg["stat_sum"]["num_bytes"]

    osd_df_osd_as_key = {osd["id"]: osd for osd in osd_df["nodes"]}

    print(
        "osd_id disk_size disk_usage acting_size acting_util -> size_then util_then reweight"
    )
    if args.osds:
        osds = args.osds
    else:
        osds = [osd.get("id") for osd in osd_df["nodes"]]

    for i in osds:
        i = int(i)
        try:
            if not args.no_change and acting_sizes[i] == sizes[i]:
                continue
            print(
                "{:>6} {:>6.2f}TiB {:>9.2f}% {:>8.2f}TiB {:>11.2%} -> {:>6.2f}TiB {:>9.2%} {:>8.6f}".format(
                    i,  # osd_id
                    osd_df_osd_as_key[i]["kb"] / 1024**3,  # disk_size
                    osd_df_osd_as_key[i]["utilization"],  # (actual) disk_usage
                    acting_sizes[i] / 1024**4,  # acting_size
                    acting_sizes[i]
                    / 1024.0
                    / osd_df_osd_as_key[i]["kb"],  # acting_util
                    sizes[i] / 1024.0**4,  # size_then
                    sizes[i] / 1024.0 / osd_df_osd_as_key[i]["kb"],  # util_then
                    osd_df_osd_as_key[i]["reweight"],  # reweight
                )
            )
        except (KeyError, ZeroDivisionError):
            print("osd.{} not found".format(i), file=stderr)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Show how disk usage will have changed after backfilling is done."
    )
    parser.add_argument("osds", nargs="*", default=[], help="Limit to Ceph OSDs")
    parser.add_argument(
        "--no-change",
        action="store_true",
        default=False,
        help="Do not omit OSDs with no change",
    )
    parser.add_argument(
        "--ceph-osd-df",
        help="Output of `ceph osd df --format json`, run command if omited",
    )
    parser.add_argument(
        "--ceph-pg-dump-pgs",
        help="Output of `ceph pg dump osds --format json`, run command if omited",
    )

    args = parser.parse_args()
    main(args)

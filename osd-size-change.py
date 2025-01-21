#!/usr/bin/env python3
# Usage: ./$0 [OSD...]
# Sums up the sizes of PGs of one, more or all OSDs for the cases where the OSD
# is up or acting
from argparse import ArgumentParser
from collections import defaultdict
from json import loads
from subprocess import run


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
    pgs = json_from_file_or_command(
        args.ceph_pg_dump_pgs, "ceph pg dump pgs --format json"
    )

    sizes = defaultdict(int)
    acting_sizes = defaultdict(int)

    assert pgs.get("pg_ready", False)

    for pg in pgs.get("pg_stats", {}):
        for i in pg.get("up"):
            # if balanced
            sizes[str(i)] += pg.get("stat_sum", {}).get("num_bytes", 0)
        for i in pg.get("acting"):
            # if missplaced
            acting_sizes[str(i)] += pg.get("stat_sum", {}).get("num_bytes", 0)

    if args.osds:
        osds = args.osds
    else:
        osds = json_from_file_or_command(
            args.ceph_pg_dump_osds,
            "ceph pg dump osds --format json",
        )
        assert osds.get("pg_ready", False)
        osds = [str(osd["osd"]) for osd in osds.get("osd_stats", [])]

    print("OSD acting_size      size    change")
    for i in osds:
        if not args.no_change and acting_sizes.get(i, 0) == sizes.get(i, 0):
            continue
        if acting_sizes.get(i) or sizes.get(i):
            print(
                "{:>3} {:>8.2f}GiB {:>6.2f}GiB {:>6.2f}GiB".format(
                    i,
                    acting_sizes[i] / 1024**4,
                    sizes[i] / 1024.0**4,
                    (sizes[i] - acting_sizes[i]) / 1024.0**4,
                )
            )


if __name__ == "__main__":
    parser = ArgumentParser(
        description=(
            'Show a table of (a subset of) all Ceph OSDs and their "acting_size" (now), '
            'their "size" (future), and the change from not to then'
        )
    )
    parser.add_argument(
        "--ceph-pg-dump-pgs",
        help="Output of `ceph pg dump pgs --format json`, run command if omited",
    )
    parser.add_argument(
        "--ceph-pg-dump-osds",
        help="Output of `ceph pg dump osds --format json`, run command if omited",
    )
    parser.add_argument(
        "--no-change",
        action="store_true",
        default=False,
        help="Do not omit OSDs with no change",
    )
    parser.add_argument(
        "osds",
        nargs="*",
        default=[],
        help="Limit results to on ore more Ceph OSDs",
    )
    args = parser.parse_args()

    main(args)

#!/usr/bin/env python3
# This script searches for PGs that are not in the correct spot yet; that is
# where acting and up Ceph OSDs are not the same.
#
# You can filter by pg state but be aware that it is only a "is in states" and
# not a Regex – "backfill" will find both "backfill_wait" and "backfilling".
# Omitting the state will yield all but filtered by changes (check `--empty`)
from argparse import ArgumentParser
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


def osd_node_with_ceph_osd(ceph_osd, osd_metadata):
    for osd in osd_metadata:
        if osd["id"] == ceph_osd:
            return osd["hostname"]
    # If this state is ever reached there is something weird going on
    raise Exception("OSD node could not be found from Ceph OSD")


def main(args) -> None:
    pg_dump = json_from_file_or_command(
        args.ceph_pg_dump_pgs, "ceph pg dump pgs --format json"
    )
    osd_metadata = json_from_file_or_command(
        args.ceph_osd_metadata, "ceph osd metadata --format json"
    )

    # filter pgs that have the desired state, and add UP to ACTING diff
    filtered_pgs = []
    for pg in pg_dump["pg_stats"]:
        if args.state in pg["state"]:
            # find Ceph OSDs that change
            moving_to = list(set(pg["up"]) - set(pg["acting"]))
            moving_from = list(set(pg["acting"]) - set(pg["up"]))

            # only append something with a diff
            if args.empty or moving_to or moving_from:
                filtered_pgs.append(pg)
                pg["moving_from"] = list(set(pg["acting"]) - set(pg["up"]))
                pg["moving_to"] = list(set(pg["up"]) - set(pg["acting"]))
                pg["osd_node_moving_from"] = []
                pg["osd_node_moving_to"] = []
                # Find osd nodes where the moving PGs are on
                if pg["moving_from"]:
                    for ceph_osd in pg["moving_from"]:
                        pg["osd_node_moving_from"].append(
                            osd_node_with_ceph_osd(ceph_osd, osd_metadata)
                        )
                if pg["moving_to"]:
                    for ceph_osd in pg["moving_to"]:
                        pg["osd_node_moving_to"].append(
                            osd_node_with_ceph_osd(ceph_osd, osd_metadata)
                        )

    # Find formating information
    pgid_width = max([len(pg["pgid"]) for pg in filtered_pgs], default=1)
    acting_width = max([len(str(pg["acting"])) for pg in filtered_pgs], default=1)
    up_width = max([len(str(pg["up"])) for pg in filtered_pgs], default=1)
    moving_from_width = max(
        [len(str(pg["moving_from"])) for pg in filtered_pgs] + [len("MOVING FROM")],
    )
    moving_to_width = max(
        [len(str(pg["moving_to"])) for pg in filtered_pgs] + [len("MOVING TO")]
    )
    osd_node_moving_from_width = max(
        [len(str(pg["osd_node_moving_from"])) for pg in filtered_pgs]
        + [len("OSD NODE MOVING FROM")]
    )
    osd_node_moving_to_width = max(
        [len(str(pg["osd_node_moving_to"])) for pg in filtered_pgs]
        + [len("OSD NODE MOVING TO")]
    )

    # TODO: think of better ways to format. This might need refactoring
    # to make it more easy to show and hide certain columns
    # There is probably a library for doing this way easier
    format_string = "{:<{pgid_width}} {:{acting_width}} {:{moving_from_width}} {:{osd_node_moving_from_width}} -> {:{up_width}} {:{moving_to_width}} {:{osd_node_moving_to_width}} {}"
    width_array = {
        "pgid_width": pgid_width,
        "acting_width": acting_width,
        "up_width": up_width,
        "moving_from_width": moving_from_width,
        "moving_to_width": moving_to_width,
        "osd_node_moving_from_width": osd_node_moving_from_width,
        "osd_node_moving_to_width": osd_node_moving_to_width,
    }
    # Print headers
    print(
        format_string.format(
            "PGID",
            "ACTING",
            "MOVING FROM",
            "OSD NODE MOVING FROM",
            "UP",
            "MOVING TO",
            "OSD NODE MOVING TO",
            "STATE",
            **width_array,
        )
    )
    for pg in filtered_pgs:
        print(
            format_string.format(
                pg["pgid"],
                str(pg["acting"]),
                str(pg["moving_from"]),
                str(pg["osd_node_moving_from"]),
                str(pg["up"]),
                str(pg["moving_to"]),
                str(pg["osd_node_moving_to"]),
                pg["state"],
                **width_array,
            )
        )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "state",
        default="",
        nargs="?",
        help="Filter by state, omit to get all PGs",
    )
    parser.add_argument(
        "--empty",
        action="store_true",
        default=False,
        help="Print even if acting and up are identical",
    )
    parser.add_argument("--ceph-pg-dump-pgs", help="ceph pg dump pgs --format json")
    parser.add_argument("--ceph-osd-metadata", help="ceph osd metadata --format json")

    args = parser.parse_args()
    main(args)

#!/usr/bin/env bash
# Looking at analyse-osd-size-changes find the fullest Ceph OSD and reduced its reweight by .01
# Do this over and over again every 10 seconds for $1 seconds
# The 10 seconds sleep are for giving Ceph time to calculate new PG placements.
#
# I would recommand to backup osd weight before running this.
# I would recomment not running this script for too long and not leeting it run unattended.

while [ $SECONDS -lt $1 ]; do
    worst_osd="$(./analyse-osd-size-changes.py --no-change 2>/dev/null |
        sort -nk7 | tail -1)"
    worst_osd_id=$(awk '{print $1}' <<<$worst_osd)
    current_weight=$(awk '{print $8}' <<<$worst_osd)
    new_weight=$(echo "$current_weight - 0.01" | bc -l)

    # rebalance
    echo Reweighting $worst_osd_id from $current_weight
    echo ceph osd reweight osd.${worst_osd_id} $new_weight
    # give Ceph time to calculate new placedments of PGs
    sleep 10s

    ./analyse-osd-size-changes.py --no-change | sort -nk7 | tail -5
done

./analyse-osd-size-changes.py --no-change | sort -nk7
ceph -s

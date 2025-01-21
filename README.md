# Ceph Scripts

This is a collection of some scripts I have written in the past for managing
Ceph clusters. Most of them relate to a very full cluster. I also wrote an [blog
article] with things to consider first before diving into some custom scripts
someone write on the internet.

- Documentation (and FAQ – if any) can found in this README.
- Feel free to file issues to report bugs, ask questions, or request features.
- Feel free to open a pull request. Please use the
  [black](https://github.com/psf/black) code formatter for python scripts.

## analyse-osd-size-changes.py

After manually reweighting a Ceph OSD it is not immediately apparent how the
disk usage will have changed. Knowing this allow you to make decisions on
further optimizations on weights.

Example output:

```text
osd_id disk_size disk_usage acting_size acting_util -> size_then util_then reweight
   219   9.27TiB     60.94%     5.41TiB      58.35% ->   5.48TiB    59.14% 1.000000
   241   9.27TiB     58.10%     5.29TiB      57.05% ->   5.22TiB    56.27% 1.000000
   240   9.27TiB     59.72%     5.29TiB      57.12% ->   5.37TiB    57.90% 1.000000
   181   9.27TiB     57.53%     5.24TiB      56.52% ->   5.17TiB    55.73% 1.000000
   180   9.27TiB     60.48%     5.37TiB      57.90% ->   5.44TiB    58.69% 1.000000
   220   9.27TiB     56.01%     5.10TiB      54.99% ->   5.03TiB    54.21% 1.000000
```

## optimize-reweights-based-on-analyse-osd-size-changes.sh

We can use [analyse-osd-size-changes.py](#analyse-osd-size-changespy) to find
the fullest Ceph OSD and reweight it down a little. Then we can wait for a few
seconds for Ceph to calculate PG movements. Rinse and repeat.

To do this process every ten seconds for `$1` seconds, you can use
`./optimize-reweights-based-on-analyse-osd-size-changes.sh`. It reweights the
(soon to be) fullest Ceph OSD -.01 its current weight, waits ten seconds and
starts over.

This is a pretty dumb script with little safeguards (e.g. reweighting below 0).
It should not get run automatically or unattended. And you should backup your
Ceph OSD weights.

## force-important-backfills.py

Ceph is smart enough to do the backfills and recoveries it can and does
[prioritize] based on PG state. Sometimes you sill might want to speed up
backfilling from full Ceph OSDs to less full ones manually though.

This script looks at the Ceph OSDs utilization and picks the highest ones to
force backfilling.

## pg-movements.py

Show where PGs will move.

```console
❯ ./pg-movements.py
PGID  ACTING          MOVING FROM OSD NODE MOVING FROM -> UP              MOVING TO  OSD NODE MOVING TO   STATE
5.fff [181, 152, 161] [181]       ['ceph10']           -> [180, 152, 161] [180]      ['ceph12']           active+clean
5.ff9 [100, 241, 220] [241, 220]  ['ceph08', 'ceph12'] -> [100, 240, 219] [240, 219] ['ceph09', 'ceph08'] active+clean
```

## osd-size-change.py

Show how sizes change after current backfilling is complete.

```console
❯ ./osd-size-change.py
OSD acting_size      size    change
241     5.29GiB   5.22GiB  -0.07GiB
240     5.29GiB   5.37GiB   0.07GiB
220     5.10GiB   5.03GiB  -0.07GiB
219     5.41GiB   5.48GiB   0.07GiB
181     5.24GiB   5.17GiB  -0.07GiB
180     5.37GiB   5.44GiB   0.07GiB
```

[blog article]: g
[prioritize]: https://docs.ceph.com/en/latest/dev/osd_internals/backfill_reservation/

# Per-day tactical rosters

One CSV per event day, columns `callsign,tactical`, derived from the Prouty
APRS assignment sheet (the "APRS" device column -> the role name).

These files are the roster for a run: pass `--roster config/<day>.csv` and the
tools take BOTH the callsign list and the tactical labels from it, so there is
no separate `--calls` string to maintain. The same physical callsign can carry
a different tactical name each day (e.g. W1CMS-8 is Safety on Friday, Rover 6-7
on Saturday), which is why the roster is per day.

Only assigned rover callsigns are listed; fixed infrastructure (N1GMC-1/2,
W1FN sites, WX1NH-10) is excluded. Trackers that beaconed on a day without an
assignment (incidental/testing) are intentionally absent, so a roster-driven
run analyzes only that day's actual rovers.

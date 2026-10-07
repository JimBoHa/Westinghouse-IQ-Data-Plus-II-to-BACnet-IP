# BACnet, recording and diagnostics

The installer separates the source checkout from state and data. Its default
data directory is `~/.local/share/iqdata`; replace that path below if configured
otherwise. Each service runs as the installing user. No service needs root.

```sh
systemctl --user status iqdata-poll iqdata-bacnet iqdata-http iqdata-snapshot.timer
journalctl --user -u iqdata-poll -u iqdata-bacnet -n 40 --no-pager
cat "$HOME/.local/share/iqdata/state/bacnet-status.json"
```

Only the poller opens Pico USB. BACnet and HTTP read atomic state files, so
client requests do not cause faster meter polling. Stop `iqdata-poll.service`
before firmware maintenance or manual USB commands; restart it afterwards.

## BACnet discovery and points

Use a unique device instance and your Pi's real address/prefix. On the same
subnet, normal BACnet/IP uses UDP47808 with broadcast reception. The gateway
answers Who-Is with range filtering and directed replies and announces at
startup/every60seconds. `--metasys` optionally adds directed I-Am to a supervisor;
it is not required for ordinary discovery. In Metasys, discover under BACnet
Integration, add the device, then refresh field-point discovery.

The default point map contains **198 points plus Device and Network Port**.
The [CSV](../live/BACNET_POINT_MAP.csv) uses example device instance75151; the
installer's configured device instance replaces it. Point instances stay stable.

| Group | Objects |
| --- | --- |
| Live electrical measurements and estimates | AI1–18 |
| Sample age, live electrical health, display-verified flag | AI20, BI1–2 |
| Current alarm/trip/cause flags | BI10–18 |
| Retained trip readings and qualification | AI100–114, BI30–39 |
| Firmware and configuration/switch observations | AI200–207,210–222; BI100–147,150–166 |
| Software diagnostics and readiness | AI300–341, BI200–205 |
| Poll communication diagnostics | AI400–408 |

The interface supports ReadProperty, ReadPropertyMultiple and confirmed or
unconfirmed COV. Writes are denied. Subscriptions persist in
`state/bacnet-cov-subscriptions.json`; restart preserves unexpired leases without
extending them. Do not copy this cache between installations: it contains client
addresses and subscriptions. Version1.1.0 uses databaseRevision2.

Missing, stale or invalid data has `Status_Flags.fault` and
`Reliability=communication-failure`. Last good Present_Value may remain visible;
startup defaults are zero **with fault set**. Always check quality. Rolling
warmup also uses fault quality, with separate readiness BIs. BI1 covers original
electrical readings, not every diagnostic point. Group maximum ages are5seconds
for live/derived values,30seconds for flags,1hour for settings,3minutes for trip
buffer acquisition, and10seconds for poll health. Trip read age is not event age.

RMS phase imbalance is maximum deviation from the phase mean, not negative
sequence unbalance. Session statistics and rolling windows reset on gateway
restart. Rolling demand/load factor need15minutes of continuous valid coverage;
energy windows need1hour/24hours. Gaps, invalid data and counter decreases break
relevant coverage. Software thresholds are PF magnitude0.90, frequency±0.5Hz,
and voltage±10% of the meter's readable nominal setting; these actuate nothing.

## SQLite and HTTP

Standard measurements, raw words, timestamps and failures are committed to
`iqdata.sqlite` using WAL and synchronous FULL. Normal request slots are1.05s;
flags/settings/trip consume occasional separate slots. New diagnostics overwrite
JSON state and are **not trended**. Bounded per-request USB journals remain in
`state/polls`; storage grows continuously until you archive/remove old journals.
The complete measurement database can grow around1.6GB/day at this cadence;
the downloadable snapshot requires a second copy. Actual growth varies.

The snapshot timer uses SQLite's online backup API hourly, at reduced CPU and I/O
priority. Each job reports its duration and checks the copy before publishing it.
The copy and validation share a 25-minute cooperative budget (`db_snapshot.py
--max-seconds 1500`). Generated units allow 30 minutes overall and 60 seconds
for shutdown, so large databases are not killed after five minutes. Adjust both
limits together as the database grows; keep the application budget below the
service timeout. An expired budget preserves the previously published download.
The acquisition database still records every measurement attempt. To refresh the
download early, run `systemctl --user start iqdata-snapshot.service`. Download
`http://PI_ADDRESS:8080/database`, or copy `iqdata-latest.sqlite`. Do not copy
only the live `.sqlite` file while its WAL is active. For a fresh snapshot:

```sh
systemctl --user start iqdata-snapshot.service
.venv/bin/python live/store_readings.py status \
  --database "$HOME/.local/share/iqdata/iqdata.sqlite"
```

`/telemetry` serves electrical values; `/status` serves saved acquisition state.
HTTP is read-only but has no login or TLS, and BACnet/IP is unencrypted. Keep
these ports on the intended controls LAN; do not forward them to the Internet.

`configure_recording.py` creates convenience views and metadata after the
first database exists. Query `samples` for a wide table or `scalar_readings`
for long-form values. Source conventions and units are described in
[PROTOCOL.md](PROTOCOL.md). THD is unavailable, not inferred from PF.

## External verification

From another host with the dependencies installed, substitute its own local
IPv4/prefix and a spare client device instance. Broadcast tests use the same
UDP port as the gateway:

```sh
.venv/bin/python live/probe_bacnet.py \
  --address CLIENT_ADDRESS/24:47808 --instance 4194001 --name IQData-Test-Client \
  --target PI_ADDRESS --device 75151 --output /tmp/iqdata-bacnet-check.json
```

The bounded probe checks discovery, all point properties, same-value write
rejection and temporary COV, then cancels its subscription. A same-value Device
name write is intentionally rejected; no meter command is sent by this probe.

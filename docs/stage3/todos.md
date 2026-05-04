# Stage 3: How to Think About the TODOs

Four TODO blocks across the producer + consumer notebooks. This doc explains *what each one is asking you to decide* and *what to consider*, without giving you the answer. When you're done with one, run the cell — the `assert` will tell you if your shape is reasonable, but won't grade your judgment.

---

## TODO #1 — Producer rate (`03a-streaming-producer.ipynb`)

```python
EVENTS_PER_SEC = None   # int in [1, 500]
MAX_EVENTS     = None   # int in [500, 50000]
```

**The question:** how fast should the producer publish, and for how long?

**Trade-offs:**
- Higher `EVENTS_PER_SEC` → 2-minute windows fill faster, more alerts in less wall time, more CPU on your laptop.
- Lower `EVENTS_PER_SEC` → cleaner demo, less broker load, longer wait for first alert.
- `MAX_EVENTS` controls total runtime: `runtime_minutes = MAX_EVENTS / EVENTS_PER_SEC / 60`.

**Sanity targets:**
- Aim for 5–10 minutes of producer runtime so the consumer's 2-min windows complete and slide a few times.
- If you'll demo this live in front of a screen-share, you want something that produces visible alert lines within the first 3 minutes — that means rate × ~180 ≥ enough events to fill at least one window for some session.

**What "wrong" looks like:**
- 500 ev/s for 50000 events → finishes in 100 seconds, the consumer barely sees one window slide. Picks too short a demo.
- 1 ev/s for 50000 events → ~14 hour run. Too slow.

---

## TODO #2 — Alert thresholds (`03b-streaming-consumer.ipynb`)

```python
LATENCY_ALERT_MS       = None
PACKET_LOSS_ALERT_PCT  = None
AUDIO_QUALITY_ALERT    = None
```

**The question:** at what averaged-over-2-minutes values do you fire an alert?

**Defaults from the brief:** `latency > 500ms`, `packet_loss > 5%`. The brief is silent on audio quality (codec score 1–10) — that's your call.

**Why you might deviate from the defaults:**
- The defaults are *single-sample* thresholds (Stage 2 batch checks them per session row). Stage 3 averages over 2 minutes — averaging smooths out spikes, so applying the same thresholds to averages will fire fewer alerts. Some teams lower them (e.g. `latency_ms > 400`) to compensate.
- If your producer's data has very few sessions exceeding 500 ms, you'll see zero alerts during the demo. Lowering thresholds is one fix; a `degraded_session` data injector in the producer is another.

**Things to mention in your write-up:**
- *Why* you picked the numbers you did. "Same as Stage 2" is fine if you also explain the averaging effect.
- The trade-off between false positives (annoying patients with bogus alerts) and false negatives (missing actual degradations).

---

## TODO #3 — `alert_type` classifier (`03b`, cell 6)

```python
alert_type = None  # F.when(...).when(...).otherwise(...) chain
```

**The question:** when a window crosses a threshold, what string label do you put on the alert?

**Three families of choice:**

### (a) Single most-severe (recommended)

Pick one label per window — the most actionable. Example logic: if `avg_packet_loss_pct > threshold`, that's worse than just high latency, so call it `'packet_loss'`. If only audio is bad, call it `'low_audio'`.

```python
F.when(<packet loss breach>, F.lit('packet_loss'))
 .when(<latency breach>,     F.lit('high_latency'))
 .when(<audio breach>,       F.lit('low_audio'))
 .otherwise(F.lit('unknown'))
```

Best for routing — downstream knows which subscriber list to hit.

### (b) Combined string

`'high_latency+packet_loss'` for multi-breach windows. Easy to grep, but downstream code has to parse it.

### (c) Array of all breached types

```python
F.array_remove(
    F.array(
        F.when(<latency breach>,     F.lit('high_latency')),
        F.when(<packet loss breach>, F.lit('packet_loss')),
        F.when(<audio breach>,       F.lit('low_audio'))),
    None)
```

Closest to Stage 2's `degraded_severity`. Easier to count + analyze, harder to switch on.

**Which is right?** Depends on what the downstream alert consumer does with it. For a notification service that pages on-call engineers, (a) is best. For an analytics dashboard, (c) is more useful. Pick one and write a sentence in your report on why.

**Brief reference:** the brief gives `{high_latency, packet_loss, low_resolution}` as example types. (We don't have video resolution as a continuous metric, so `low_audio` is the natural substitute.)

---

## TODO #4 — `suggested_action` mapping (`03b`, cell 8)

```python
suggested_action = None  # F.when(...).otherwise(...)
```

**The question:** what does the alert tell the patient or physician to do?

This is the user-facing part. The alert itself is just a JSON blob — `suggested_action` is the human-readable instruction that would appear in the UI.

**Examples of good, actionable suggestions:**
- High latency → `'reduce video resolution to 480p'` or `'pause video for 30 seconds and try again'`
- Packet loss → `'switch to audio-only mode'` or `'check wifi signal strength'`
- Low audio → `'check headset/microphone connection'` or `'switch to a different audio device'`

**Examples of weak suggestions:**
- `'quality is bad'` (not actionable)
- `'restart your computer'` (overkill, blames the user)
- `'contact MediStream support'` (passive, doesn't fix the call)

**Fancier (optional):** condition on `device_type` too. A phone with packet loss might benefit from `'switch from cellular to wifi'`; a laptop with the same condition can't.

**Style:** short imperative sentences. Pretend you're writing the actual UI string a patient sees during their visit.

---

## After all four TODOs are filled

Run all the cells of `03b-streaming-consumer.ipynb` top to bottom. The streaming query should start, print `Streaming query started. id = ...`, and then sit at `awaitTermination()` waiting for events.

In a separate browser tab, run `03a-streaming-producer.ipynb` end to end. After ~30 seconds you should see lines like:

```
[batch 0] emitting 12 alert rows
[batch 1] emitting 8 alert rows
...
```

After a few minutes, run `03c-streaming-health-check.ipynb` to verify topics, offsets, and HDFS Parquet are all sane.

If something doesn't fire alerts, the most common cause is thresholds set too high for the data. Try lowering them by 20% and re-running.

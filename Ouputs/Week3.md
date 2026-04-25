
```table
# ── BLOCK 4: Incremental load — watermark pattern ────────────

=== Incremental load — watermark: 2024-01-17 10:00:00 ===
New records since last run: 3
+------+-----------+-------+----------+---------+-------------------+
|txn_id|customer_id| amount|  txn_date|   status|         updated_at|
+------+-----------+-------+----------+---------+-------------------+
|     6|       C001|1299.99|2024-01-18|completed|2024-01-18 08:00:00|
|     8|       C002| 449.99|2024-01-19|completed|2024-01-19 09:00:00|
|     7|       C003|  89.99|2024-01-18|  pending|2024-01-18 08:30:00|
+------+-----------+-------+----------+---------+-------------------+


=== Watermark updated to: 2024-01-19 09:00:00 ===

=== Bronze table — all ingested records ===
+------+-----------+-------+----------+---------+-------------------+
|txn_id|customer_id| amount|  txn_date|   status|         updated_at|
+------+-----------+-------+----------+---------+-------------------+
|     1|       C001| 999.99|2024-01-15|completed|2024-01-15 08:00:00|
|     2|       C002| 699.99|2024-01-15|completed|2024-01-15 08:05:00|
|     3|       C001| 499.99|2024-01-16|  pending|2024-01-16 09:00:00|
|     4|       C003| 299.99|2024-01-16|completed|2024-01-16 09:10:00|
|     5|       C002| 149.99|2024-01-17|completed|2024-01-17 10:00:00|
|     6|       C001|1299.99|2024-01-18|completed|2024-01-18 08:00:00|
|     7|       C003|  89.99|2024-01-18|  pending|2024-01-18 08:30:00|
|     8|       C002| 449.99|2024-01-19|completed|2024-01-19 09:00:00|
+------+-----------+-------+----------+---------+-------------------+

# ── BLOCK 5: Cloud storage read with explicit schema ─────────
path	name	size	modificationTime
dbfs:/tmp/landing/orders/orders_day1.csv	orders_day1.csv	155	1777094651000
dbfs:/tmp/landing/orders/orders_day2.csv	orders_day2.csv	142	1777094651000

# ── BLOCK 6: Read with explicit schema + corrupt record handling

=== Raw read — all rows including corrupt ===
+--------+-----------+------+----------+---------+--------------------+
|order_id|customer_id|amount|order_date|   status|     _corrupt_record|
+--------+-----------+------+----------+---------+--------------------+
|     101|       C001| 250.0|2024-01-15|completed|                NULL|
|     102|       C002| 175.5|2024-01-15|completed|                NULL|
|     103|       C003|  NULL|2024-01-15|  pending|103,C003,INVALID,...|
|     104|       C001| 320.0|2024-01-16|completed|104,C001,320.00,2...|
|     105|       C002|  95.0|2024-01-16|  pending|105,C002,95.00,20...|
+--------+-----------+------+----------+---------+--------------------+


Clean records: 5
Corrupt records: 0

=== Clean orders ===
+--------+-----------+------+----------+---------+
|order_id|customer_id|amount|order_date|   status|
+--------+-----------+------+----------+---------+
|     101|       C001| 250.0|2024-01-15|completed|
|     102|       C002| 175.5|2024-01-15|completed|
+--------+-----------+------+----------+---------+


=== Corrupt records (quarantined) ===
+---------------+
|_corrupt_record|
+---------------+
+---------------+

---Bock 7

=== FAILFAST mode — fails on first corrupt record ===
FAILFAST caught error (expected): PySparkTypeError
Pipeline stopped on first corrupt record
Use FAILFAST when data must be perfect — zero tolerance for bad records

=== Read modes summary ===
PERMISSIVE  — capture bad rows in _corrupt_record, continue
DROPMALFORMED — silently skip bad rows, continue
FAILFAST    — stop immediately on first bad row


```

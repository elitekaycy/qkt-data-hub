"""qkt-data-hub: a point-in-time fact store for trading systems.

The hub acquires non-price information a strategy can react to -- scheduled releases and their
consensus, published series and their revisions, venue facts -- and writes it as one universal
append-only record format. A consumer standing at time T sees only records whose `known_at` is
at or before T, which is the single correctness property the whole product protects.

Live and history are the same bytes: the journal a collector appends to is the input the
snapshot compiler folds, so a backtest and a live session read one format, not two pipelines.
"""

__version__ = "0.1.4"

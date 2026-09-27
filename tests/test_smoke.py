"""End-to-end smoke test: one live check per layer of the crawler.

This is deliberately not a contract suite. Each test names the layer and does
the smallest real thing that layer must be able to do, so a failure here points
straight at the broken layer instead of at a detail.

Run with `-m smoke` to execute just these.
"""

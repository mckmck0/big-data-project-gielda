"""Streaming analytics for the commodity exchange module (generator-gielda).

Package layout:
    config     -- properties/env configuration and Flink environment setup.
    schemas    -- shared type information and state descriptors.
    utils      -- parsing and watermarking helpers.
    sources    -- Kafka source builders.
    pipelines  -- processing stages (dictionary broadcast, level 1, level 2, alerts).
"""

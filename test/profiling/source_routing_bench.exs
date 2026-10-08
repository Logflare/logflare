System.put_env("ROUTING_BENCH_BATCHES", System.get_env("ROUTING_BENCH_BATCHES", "1"))
Code.require_file("source_routing_scale_bench.exs", __DIR__)

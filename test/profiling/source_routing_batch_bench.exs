System.put_env("ROUTING_BENCH_BATCHES", System.get_env("ROUTING_BENCH_BATCHES", "10,100"))
System.put_env("ROUTING_BENCH_COMPARE_BATCH", "1")
Code.require_file("source_routing_scale_bench.exs", __DIR__)

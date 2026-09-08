# Usage: mix run bench/mapper_flat_keys_elevate.exs
#
# Covers the `flat_keys: true` branch of `Logflare.Mapper`, where the input is
# already flattened to dot-notation keys and `elevate_keys` matches on key
# prefixes rather than nested maps. That branch has no production caller today —
# nothing in `lib/` passes `flat_keys: true` — so this bench exists to give the
# path a baseline before something starts using it.
#
# Three input shapes drive the three distinct code paths in
# `apply_multiple_elevate_keys_flat`:
#
#   * single_group — only `metadata.*` prefixes match, so elevated suffixes are
#     unique and the result map is built in one `map_from_term_arrays` call
#   * multi_group  — `metadata.*` and `attributes.*` both match with disjoint
#     suffixes, taking the reverse-group insert path
#   * collide      — both prefixes match and produce the *same* suffixes, so the
#     earlier configured elevate key has to win each collision

alias Logflare.Mapper
alias Logflare.Mapper.MappingConfig
alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field

compiled =
  Mapper.compile!(
    MappingConfig.new([
      Field.flat_map("attrs",
        path: "$",
        exclude_keys: ["id", "event_message", "timestamp"],
        elevate_keys: ["metadata", "attributes"]
      )
    ])
  )

envelope = %{"id" => "uuid-1", "event_message" => "User logged in", "timestamp" => 1_769_018_088}

top_level = for i <- 1..40, into: %{}, do: {"top_key_#{i}", "some value #{i}"}

prefixed = fn prefix, range, label ->
  for i <- range, into: %{}, do: {"#{prefix}.field_#{i}", "#{label} #{i}"}
end

single_group =
  top_level
  |> Map.merge(prefixed.("metadata", 1..40, "meta"))
  |> Map.merge(envelope)

multi_group =
  top_level
  |> Map.merge(prefixed.("metadata", 1..20, "meta"))
  |> Map.merge(prefixed.("attributes", 21..40, "attr"))
  |> Map.merge(envelope)

collide =
  top_level
  |> Map.merge(for i <- 1..20, into: %{}, do: {"metadata.shared_#{i}", "meta #{i}"})
  |> Map.merge(for i <- 1..20, into: %{}, do: {"attributes.shared_#{i}", "attr #{i}"})
  |> Map.merge(envelope)

# Sanity: confirm each shape produces the expected key count and that the
# earlier elevate key wins a suffix collision.
# credo:disable-for-lines:12
for {label, doc, expected} <- [
      {"single_group", single_group, 80},
      {"multi_group", multi_group, 80},
      {"collide", collide, 60}
    ] do
  out = Mapper.map(doc, compiled, flat_keys: true)["attrs"]

  IO.puts(
    "#{String.pad_trailing(label, 14)} in=#{map_size(doc)} out=#{map_size(out)} (expected #{expected})"
  )
end

collide_out = Mapper.map(collide, compiled, flat_keys: true)["attrs"]
IO.puts("collision resolved to metadata: #{collide_out["shared_1"] == "meta 1"}\n")

Benchee.run(
  %{
    "single_group (one prefix matches)" => fn ->
      Mapper.map(single_group, compiled, flat_keys: true)
    end,
    "multi_group (two prefixes, disjoint suffixes)" => fn ->
      Mapper.map(multi_group, compiled, flat_keys: true)
    end,
    "collide (two prefixes, same suffixes)" => fn ->
      Mapper.map(collide, compiled, flat_keys: true)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3,
  print: [configuration: false]
)

# Baseline results — Apple M4 / 32 GB / macOS / Elixir 1.19.5 / Erlang 27.3.4.6
# Recorded after tidying `apply_multiple_elevate_keys_flat` to size its vectors
# from `map_size/1` and bulk-build the base map when only one prefix matched.
#
# Name                                                    ips     average   memory   reductions
# single_group (one prefix matches)                   56.37 K    17.74 μs  2.34 KB          229
# multi_group (two prefixes, disjoint suffixes)       41.71 K    23.98 μs  2.32 KB          426
# collide (two prefixes, same suffixes)               40.57 K    24.65 μs  4.85 KB          390
#
# Same-checkout A/B against the previous implementation, which used
# `vec![Vec::new(); n]` plus per-entry `map_put` for every case:
#
#   single_group   364 -> 229 reductions
#   multi_group    426 -> 426 reductions
#   collide        390 -> 390 reductions
#
# The bulk build is worth ~37% of the reductions when only one prefix matches.
# Both multi-prefix cases are unchanged — the reverse-group scan costs the same as
# the per-group vectors it replaced. Compare reductions rather than wall time;
# reductions are deterministic and this bench swings ±16-30% on time.
#
# Re-run after switching elevated suffix keys from `encode_string` (which ran
# `from_utf8(...).unwrap_or("")` and silently renamed any non-UTF-8 suffix to "")
# to `encode_binary`, which copies the suffix bytes verbatim:
#
#   single_group   229 -> 229 reductions   2.34 KB -> 2.34 KB
#   multi_group    426 -> 426 reductions   2.32 KB -> 2.32 KB
#   collide        390 -> 390 reductions   4.85 KB -> 4.85 KB
#
# No penalty — identical on both deterministic measures. The UTF-8 validation
# pass over each suffix is gone, replaced by a straight byte copy, so if anything
# there is marginally less work per elevated key.

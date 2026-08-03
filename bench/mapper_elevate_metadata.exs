# Usage: mix run bench/mapper_elevate_metadata.exs
#
# Validates that preserving a non-map `metadata` value as a literal key in the
# `Logflare.Mapper` NIF does not regress the hot path. `log_attributes` is
# mapped for every ingested ClickHouse event (~2M/s in prod), so the elevate
# step must stay tight.
#
# Three input shapes exercise the elevate branch:
#   * metadata_map    — `metadata` is a nested map, children elevated (unchanged path)
#   * metadata_string — `metadata` is a serialized string, now preserved (fixed path)
#   * no_metadata     — no `metadata` key, elevate is a no-op (baseline)

alias Logflare.Mapper
alias Logflare.Mapper.MappingConfig
alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field

config =
  MappingConfig.new([
    Field.flat_map("log_attributes",
      path: "$",
      exclude_keys: ["id", "event_message", "timestamp"],
      elevate_keys: ["metadata"]
    )
  ])

compiled = Mapper.compile!(config)

base_attrs = fn ->
  for i <- 1..12, into: %{}, do: {"attribute_key_#{i}", "some attribute value #{i}"}
end

metadata_map =
  base_attrs.()
  |> Map.merge(%{
    "id" => "uuid-1",
    "event_message" => "User logged in",
    "timestamp" => 1_769_018_088_144_506_000,
    "metadata" => %{"region" => "us-east-1", "host" => "web-01", "reason" => "abuse"}
  })

metadata_string =
  base_attrs.()
  |> Map.merge(%{
    "id" => "uuid-1",
    "event_message" => "User logged in",
    "timestamp" => 1_769_018_088_144_506_000,
    "metadata" => ~s({"region":"us-east-1","host":"web-01","reason":"abuse","actor":"admin"})
  })

no_metadata =
  base_attrs.()
  |> Map.merge(%{
    "id" => "uuid-1",
    "event_message" => "User logged in",
    "timestamp" => 1_769_018_088_144_506_000
  })

# Sanity: confirm the metadata string is preserved and the map is elevated.
string_attrs = Mapper.map(metadata_string, compiled)["log_attributes"]
map_attrs = Mapper.map(metadata_map, compiled)["log_attributes"]

IO.puts("metadata_string preserved: #{Map.get(string_attrs, "metadata") != nil}")
IO.puts("metadata_map elevated (region): #{Map.get(map_attrs, "region") != nil}")
IO.puts("metadata_map key dropped: #{not Map.has_key?(map_attrs, "metadata")}\n")

Benchee.run(
  %{
    "metadata_map (elevated)" => fn -> Mapper.map(metadata_map, compiled) end,
    "metadata_string (preserved)" => fn -> Mapper.map(metadata_string, compiled) end,
    "no_metadata (baseline)" => fn -> Mapper.map(no_metadata, compiled) end
  },
  time: 3,
  warmup: 1,
  memory_time: 1,
  print: [configuration: false]
)

alias Logflare.LogEvent

import Logflare.Factory

# Setup test data
user = insert(:user)
source = insert(:source, user: user)

# Generate deeply nested structure with 100 fields at 5 levels deep
# Each field at the top level will nest down 5 levels
defmodule NestedDataGenerator do
  def generate_nested(depth, field_index) when depth == 1 do
    %{
      "value" => "leaf_value_#{field_index}",
      "type" => "leaf",
      "index" => field_index
    }
  end

  def generate_nested(depth, field_index) do
    %{
      "level_#{depth}_field_a" => "value_a_#{field_index}_#{depth}",
      "level_#{depth}_field_b" => "value_b_#{field_index}_#{depth}",
      "level_#{depth}_nested" => generate_nested(depth - 1, field_index)
    }
  end
end

deeply_nested_params = %{
  "message" => "Log with 100 fields, 5 levels deep",
  "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
  "metadata" =>
    Map.new(1..100, fn i ->
      {"field_#{i}", NestedDataGenerator.generate_nested(5, i)}
    end)
}

Benchee.run(
  %{
    "100 fields with 5 level nesting" => fn ->
      LogEvent.make(deeply_nested_params, %{source: source})
    end,
    # "100 fields with 5 level nesting (map_merge: true)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source}, map_merge: true)
    # end
    # "100 fields with 5 level nesting (no_params: true)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source}, no_params: true)
    # end,
    # "100 fields with 5 level nesting (no_sys_uint: true)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source}, no_sys_uint: true)
    # end,
    # "100 fields with 5 level nesting (no_cast_embeds: true)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source}, no_cast_embeds: true)
    # end,
    # "100 fields with 5 level nesting (no_source: true)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source}, no_source: true)
    # end,
    # "100 fields with 5 level nesting (all opts)" => fn ->
    #   LogEvent.make(deeply_nested_params, %{source: source},
    #     no_params: true,
    #     no_sys_uint: true,
    #     no_cast_embeds: true,
    #     no_source: true
    #   )
    # end
  },
  time: 4,
  warmup: 1,
  memory_time: 3,
  reduction_time: 3
)

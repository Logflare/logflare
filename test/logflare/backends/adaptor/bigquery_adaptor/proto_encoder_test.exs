defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ProtoEncoderTest do
  use ExUnit.Case, async: true

  alias Google.Protobuf.DescriptorProto
  alias Google.Protobuf.FieldDescriptorProto
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ProtoEncoder
  alias Logflare.TestProtobuf.BigQueryRow
  alias Logflare.TestProtobuf.BigQueryRow.KeyValue

  @fields [
    {"id", :string, true},
    {"severity", :integer, false},
    {"score", :float, false},
    {"ok", :boolean, false},
    {"ts", :timestamp, false},
    {"attributes", :string_map, false},
    {"counts", :integer_array, false},
    {"bounds", :float_array, false},
    {"names", :string_array, false},
    {"events", :json_array, false}
  ]

  describe "descriptor/1" do
    test "numbers fields by position and maps types and labels" do
      assert %DescriptorProto{name: "Row", field: fields, nested_type: [key_value]} =
               ProtoEncoder.descriptor(@fields)

      assert Enum.map(fields, &{&1.name, &1.number, &1.label, &1.type, &1.type_name}) == [
               {"id", 1, :LABEL_REQUIRED, :TYPE_STRING, nil},
               {"severity", 2, :LABEL_OPTIONAL, :TYPE_INT64, nil},
               {"score", 3, :LABEL_OPTIONAL, :TYPE_DOUBLE, nil},
               {"ok", 4, :LABEL_OPTIONAL, :TYPE_BOOL, nil},
               {"ts", 5, :LABEL_OPTIONAL, :TYPE_INT64, nil},
               {"attributes", 6, :LABEL_REPEATED, :TYPE_MESSAGE, "KeyValue"},
               {"counts", 7, :LABEL_REPEATED, :TYPE_INT64, nil},
               {"bounds", 8, :LABEL_REPEATED, :TYPE_DOUBLE, nil},
               {"names", 9, :LABEL_REPEATED, :TYPE_STRING, nil},
               {"events", 10, :LABEL_REPEATED, :TYPE_STRING, nil}
             ]

      assert %DescriptorProto{
               name: "KeyValue",
               field: [
                 %FieldDescriptorProto{name: "key", number: 1, label: :LABEL_REQUIRED},
                 %FieldDescriptorProto{name: "value", number: 2, label: :LABEL_OPTIONAL}
               ]
             } = key_value
    end
  end

  describe "encode_row/2" do
    test "encodes every field type as protobuf the descriptor describes" do
      row = %{
        "id" => "a",
        "severity" => -9,
        "score" => 1.5,
        "ok" => true,
        "ts" => 1_761_200_000_123_456,
        "attributes" => %{"http.route" => "/api", "user.id" => "5"},
        "counts" => [1, 0, 2],
        "bounds" => [5, 10.5],
        "names" => ["exception"],
        "events" => [%{"exception.message" => "boom"}, ~s({"a":1})]
      }

      assert decode!(row) == %BigQueryRow{
               id: "a",
               severity: -9,
               score: 1.5,
               ok: true,
               ts: 1_761_200_000_123_456,
               attributes: [
                 %KeyValue{key: "http.route", value: "/api"},
                 %KeyValue{key: "user.id", value: "5"}
               ],
               counts: [1, 0, 2],
               bounds: [5.0, 10.5],
               names: ["exception"],
               events: [~s({"exception.message":"boom"}), ~s({"a":1})]
             }
    end

    test "writes missing and nil values as NULL or empty lists" do
      assert decode!(%{"id" => "a", "severity" => nil, "counts" => nil}) == %BigQueryRow{
               id: "a",
               attributes: [],
               counts: [],
               bounds: [],
               names: [],
               events: []
             }
    end

    test "replaces invalid UTF-8 in strings and map entries" do
      assert %BigQueryRow{id: "�", attributes: [%KeyValue{key: "k", value: "�"}]} =
               decode!(%{"id" => <<0xFF>>, "attributes" => %{"k" => <<0xFE>>}})
    end

    test "returns an error for a missing required value or a value of the wrong type" do
      for {row, error} <- [
            {%{}, "missing required field 'id'"},
            {%{"id" => "a", "severity" => "9"}, "invalid integer value for field 'severity'"},
            {%{"id" => "a", "severity" => 1.5}, "invalid integer value for field 'severity'"},
            {%{"id" => "a", "ts" => 2 ** 63}, "invalid timestamp value for field 'ts'"},
            {%{"id" => "a", "ok" => "true"}, "invalid boolean value for field 'ok'"},
            {%{"id" => "a", "attributes" => %{"k" => 5}},
             "invalid string_map value for field 'attributes'"},
            {%{"id" => "a", "counts" => [1, nil]},
             "invalid integer_array value for field 'counts'"},
            {%{"id" => "a", "names" => [1]}, "invalid string_array value for field 'names'"}
          ] do
        assert {:error, ^error} = ProtoEncoder.encode_row(@fields, row)
      end
    end
  end

  describe "batches/2" do
    test "keeps rows in order in batches of at most max_bytes" do
      rows = for n <- 1..10, do: String.duplicate("#{rem(n, 10)}", 4)
      batches = ProtoEncoder.batches(rows, 30)

      assert List.flatten(batches) == rows
      assert Enum.all?(batches, &(Enum.sum_by(&1, fn row -> byte_size(row) + 6 end) <= 30))
      assert length(batches) == 4
    end

    test "gives a row larger than max_bytes a batch of its own" do
      assert ProtoEncoder.batches(["a", String.duplicate("x", 100), "b"], 20) ==
               [["a"], [String.duplicate("x", 100)], ["b"]]
    end

    test "returns no batches for no rows" do
      assert ProtoEncoder.batches([]) == []
    end
  end

  defp decode!(row) do
    {:ok, encoded} = ProtoEncoder.encode_row(@fields, row)
    BigQueryRow.decode(encoded)
  end
end

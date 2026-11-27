defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ProtoEncoder do
  @moduledoc """
  Encodes rows as protobuf for the BigQuery Storage Write API against a fixed field
  list, for tables whose schema is known up front.

      descriptor = ProtoEncoder.descriptor(fields)
      {:ok, row} = ProtoEncoder.encode_row(fields, %{"id" => "a", "attributes" => %{"k" => "v"}})
      batches = ProtoEncoder.batches(rows)

  A field's position in the list is its protobuf field number, so rows only match a
  descriptor built from the same list.

  Field types map to BigQuery columns: `:timestamp` is `TIMESTAMP` as Unix
  microseconds, `:string_map` is `ARRAY<STRUCT<key STRING, value STRING>>`, and
  `:json_array` is `ARRAY<STRING>` of JSON values.
  """

  import Bitwise

  alias Google.Protobuf.DescriptorProto
  alias Google.Protobuf.FieldDescriptorProto

  @type field_type ::
          :string
          | :integer
          | :float
          | :boolean
          | :timestamp
          | :string_map
          | :integer_array
          | :float_array
          | :string_array
          | :json_array
  @type field_spec :: {String.t(), field_type(), boolean()}

  @row_message "Row"
  @key_value_message "KeyValue"
  @repeated_types [:string_map, :integer_array, :float_array, :string_array, :json_array]
  @max_batch_bytes 8 * 1024 * 1024
  @row_overhead_bytes 6
  @int64_min -(1 <<< 63)
  @int64_max (1 <<< 63) - 1

  @doc """
  Returns the writer schema for rows encoded from `fields`.
  """
  @spec descriptor([field_spec()]) :: DescriptorProto.t()
  def descriptor(fields) do
    %DescriptorProto{
      name: @row_message,
      field: fields |> Enum.with_index(1) |> Enum.map(&field_descriptor/1),
      nested_type: [key_value_descriptor()]
    }
  end

  @doc """
  Encodes `row`, whose keys are the field names.

  Missing and `nil` values are written as `NULL`, or as an empty list for list and map
  fields. Invalid UTF-8 in strings is replaced, and `:json_array` elements that are not
  strings are JSON-encoded. Returns `{:error, message}` for a missing required value or
  a value that does not match its field type.
  """
  @spec encode_row([field_spec()], map()) :: {:ok, binary()} | {:error, String.t()}
  def encode_row(fields, row) when is_map(row) do
    fields
    |> Enum.with_index(1)
    |> Enum.reduce_while([], fn {{name, type, required}, number}, acc ->
      case encode_field(number, type, Map.get(row, name)) do
        {:ok, []} when required and type not in @repeated_types ->
          {:halt, {:error, "missing required field '#{name}'"}}

        {:ok, encoded} ->
          {:cont, [acc, encoded]}

        :error ->
          {:halt, {:error, "invalid #{type} value for field '#{name}'"}}
      end
    end)
    |> case do
      {:error, _message} = error -> error
      encoded -> {:ok, IO.iodata_to_binary(encoded)}
    end
  end

  @doc """
  Groups encoded rows, in order, into batches of at most `max_bytes` each.

  The default leaves room for the descriptor and request fields within the 10 MB
  AppendRows limit. A row larger than `max_bytes` gets a batch of its own.
  """
  @spec batches([binary()], pos_integer()) :: [[binary()]]
  def batches(rows, max_bytes \\ @max_batch_bytes) when max_bytes > 0 do
    rows
    |> Enum.chunk_while(
      {[], 0},
      fn row, {batch, bytes} ->
        size = byte_size(row) + @row_overhead_bytes

        if batch != [] and bytes + size > max_bytes,
          do: {:cont, Enum.reverse(batch), {[row], size}},
          else: {:cont, {[row | batch], bytes + size}}
      end,
      fn
        {[], _bytes} -> {:cont, {[], 0}}
        {batch, _bytes} -> {:cont, Enum.reverse(batch), {[], 0}}
      end
    )
  end

  @spec field_descriptor({field_spec(), pos_integer()}) :: FieldDescriptorProto.t()
  defp field_descriptor({{name, type, required}, number}) do
    %FieldDescriptorProto{
      name: name,
      number: number,
      label: label(type, required),
      type: proto_type(type),
      type_name: if(type == :string_map, do: @key_value_message)
    }
  end

  @spec key_value_descriptor() :: DescriptorProto.t()
  defp key_value_descriptor do
    %DescriptorProto{
      name: @key_value_message,
      field: [
        %FieldDescriptorProto{name: "key", number: 1, label: :LABEL_REQUIRED, type: :TYPE_STRING},
        %FieldDescriptorProto{
          name: "value",
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING
        }
      ]
    }
  end

  defp label(type, _required) when type in @repeated_types, do: :LABEL_REPEATED
  defp label(_type, true), do: :LABEL_REQUIRED
  defp label(_type, false), do: :LABEL_OPTIONAL

  defp proto_type(type) when type in [:string, :string_array, :json_array], do: :TYPE_STRING
  defp proto_type(type) when type in [:integer, :timestamp, :integer_array], do: :TYPE_INT64
  defp proto_type(type) when type in [:float, :float_array], do: :TYPE_DOUBLE
  defp proto_type(:boolean), do: :TYPE_BOOL
  defp proto_type(:string_map), do: :TYPE_MESSAGE

  @spec encode_field(pos_integer(), field_type(), term()) :: {:ok, iodata()} | :error
  defp encode_field(_number, _type, nil), do: {:ok, []}

  defp encode_field(number, :string, value) when is_binary(value),
    do: {:ok, length_delimited(number, String.replace_invalid(value))}

  defp encode_field(number, type, value) when type in [:integer, :timestamp] do
    with {:ok, varint} <- int64_varint(value), do: {:ok, [tag(number, 0), varint]}
  end

  defp encode_field(number, :float, value) when is_number(value),
    do: {:ok, [tag(number, 1), double(value)]}

  defp encode_field(number, :boolean, value) when is_boolean(value),
    do: {:ok, [tag(number, 0), if(value, do: 1, else: 0)]}

  defp encode_field(number, :string_map, value) when is_map(value) do
    map_entries(value, fn {key, entry_value}, acc ->
      with true <- is_binary(key),
           {:ok, encoded_value} <- encode_field(2, :string, entry_value) do
        entry =
          IO.iodata_to_binary([length_delimited(1, String.replace_invalid(key)), encoded_value])

        {:cont, [acc, length_delimited(number, entry)]}
      else
        _invalid -> {:halt, :error}
      end
    end)
  end

  defp encode_field(number, :integer_array, values) when is_list(values) do
    packed(number, values, &int64_varint/1)
  end

  defp encode_field(number, :float_array, values) when is_list(values) do
    packed(number, values, fn
      value when is_number(value) -> {:ok, double(value)}
      _value -> :error
    end)
  end

  defp encode_field(number, :string_array, values) when is_list(values) do
    map_entries(values, fn
      value, acc when is_binary(value) ->
        {:cont, [acc, length_delimited(number, String.replace_invalid(value))]}

      _value, _acc ->
        {:halt, :error}
    end)
  end

  defp encode_field(number, :json_array, values) when is_list(values) do
    map_entries(values, fn value, acc ->
      case json_string(value) do
        {:ok, json} -> {:cont, [acc, length_delimited(number, json)]}
        :error -> {:halt, :error}
      end
    end)
  end

  defp encode_field(_number, _type, _value), do: :error

  defp map_entries(enumerable, fun) do
    case Enum.reduce_while(enumerable, [], fun) do
      :error -> :error
      encoded -> {:ok, encoded}
    end
  end

  defp packed(_number, [], _encode), do: {:ok, []}

  defp packed(number, values, encode) do
    with {:ok, payload} <- encode_all(values, encode) do
      {:ok, length_delimited(number, IO.iodata_to_binary(payload))}
    end
  end

  defp encode_all(values, encode) do
    map_entries(values, fn value, acc ->
      case encode.(value) do
        {:ok, encoded} -> {:cont, [acc, encoded]}
        :error -> {:halt, :error}
      end
    end)
  end

  defp json_string(value) when is_binary(value), do: {:ok, String.replace_invalid(value)}

  defp json_string(value) do
    case Jason.encode(value) do
      {:ok, json} -> {:ok, json}
      {:error, _reason} -> :error
    end
  end

  defp int64_varint(value) when is_integer(value) and value >= @int64_min and value <= @int64_max,
    do: {:ok, varint(value &&& 0xFFFFFFFFFFFFFFFF)}

  defp int64_varint(_value), do: :error

  defp double(value), do: <<value * 1.0::float-little-64>>

  defp length_delimited(number, binary), do: [tag(number, 2), varint(byte_size(binary)), binary]

  defp tag(number, wire_type), do: varint(number <<< 3 ||| wire_type)

  defp varint(value) when value < 0x80, do: <<value>>
  defp varint(value), do: <<1::1, value &&& 0x7F::7, varint(value >>> 7)::binary>>
end

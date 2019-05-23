defmodule Logflare.Validator.BigQuery.SchemaChange do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  def to_typemap(%TS{fields: fields} = schema) when is_map(schema) do
    to_typemap(fields)
  end

  def to_typemap(fields) when is_list(fields) do
    fields
    |> Enum.map(fn
      %TFS{fields: fields, name: n, type: t} ->
        k = String.to_atom(n)

        v = %{t: bq_type_to_ex(t)}

        v =
          if fields do
            Map.put(v, :fields, to_typemap(fields))
          else
            v
          end

        {k, v}
    end)
    |> Enum.into(Map.new())
  end

  def bq_type_to_ex("TIMESTAMP"), do: :datetime
  def bq_type_to_ex("RECORD"), do: :map
  def bq_type_to_ex("INTEGER"), do: :integer
  def bq_type_to_ex("STRING"), do: :string
  def bq_type_to_ex("BOOLEAN"), do: :boolean
  def bq_type_to_ex("ARRAY"), do: :list
end

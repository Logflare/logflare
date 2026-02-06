defmodule Logflare.LogEvent.Mapper do
  @moduledoc """
  Mapper for log events.
  """
  alias Logflare.LogEvent
  def caster(body) do
    {body, data} = value_mapper(body, [&calculate_values_bytes/3])
    %{
      "body"=> body,
      "id"=> LogEvent.id(body),
      # "valid"=> data.valid,
      "values_bytes"=> data.values_bytes,
    }
  end

  def value_mapper(initial_body, callbacks, initial_data \\ %{})
  def value_mapper(initial_body, callbacks, initial_data) when is_map(initial_body) do
    {body, data} = for {key, value} <- initial_body, callback <- callbacks, reduce: {%{}, initial_data} do
      {body, data} ->
          {{new_key, new_value}, new_data} = callback.({key, value}, body, data)
          if is_list(new_value) or is_map(new_value) do
            {new_list, new_data} = value_mapper(new_value, callbacks, new_data)
            {Map.put(body, new_key, new_list), new_data}
          else
            {Map.put(body, new_key, new_value), new_data}
          end
    end

  end

  def value_mapper(initial_nested_value, callbacks, initial_data) when is_list(initial_nested_value) do
    for value <- initial_nested_value,  callback <- callbacks , reduce: {[], initial_data} do
      {list_acc, data} ->
        {new_value, new_data} = callback.(value, list_acc, data)
        if is_list(new_value) or is_map(new_value) do
          value_mapper(new_value, callbacks, new_data)
        else
          {[new_value | list_acc], new_data}
      end
    end
  end


  defp calculate_values_bytes({k, v}, _body, data) do
    {{k, v}, Map.update(data, :values_bytes, :erlang.external_size(v), &(&1 + :erlang.external_size(v)))}
  end
end

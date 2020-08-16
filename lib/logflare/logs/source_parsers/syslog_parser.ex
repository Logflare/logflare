defmodule Logflare.Logs.SyslogParser do
  import NimbleParsec
  import Logflare.Logs.SyslogParser.Helpers
  alias Logflare.Syslog

  defparsec(
    :do_parse,
    optional(
      byte_length()
      |> ignore(separator())
    )
    |> concat(priority())
    |> concat(version())
    |> ignore(separator())
    |> concat(maybe(timestamp()))
    |> ignore(separator())
    |> concat(maybe(hostname()))
    |> ignore(separator())
    |> concat(maybe(appname()))
    |> ignore(separator())
    |> concat(maybe(proc_id()))
    |> ignore(separator())
    |> concat(maybe(msg_id()))
    |> concat(
      ignore(separator())
      |> maybe(sd_element())
    )
    |> concat(
      optional(
        ignore(separator())
        |> message_text()
      )
    )
  )

  defparsec(
    :do_parse_heroku_dialect,
    byte_length()
    |> ignore(separator())
    |> concat(priority())
    |> concat(version())
    |> ignore(separator())
    |> concat(maybe(timestamp()))
    |> ignore(separator())
    |> concat(maybe(hostname()))
    |> ignore(separator())
    |> concat(maybe(appname()))
    |> ignore(separator())
    |> concat(maybe(proc_id()))
    |> ignore(separator())
    |> concat(maybe(msg_id()))
    |> concat(
      optional(
        ignore(separator())
        |> maybe(sd_element())
      )
    )
    |> concat(
      optional(
        ignore(separator())
        |> message_text()
      )
    )
  )

  def parse("") do
  end

  @doc """
  Parses incoming message string into Logflare.Syslog.Message struct.

  """
  def parse(messagestr, opts \\ []) when is_binary(messagestr) do
    parser =
      case opts[:dialect] do
        :heroku -> &do_parse_heroku_dialect/1
        nil -> &do_parse/1
      end

    # with {:ok, tokens, "", _, _, _} <- parser.(messagestr) |> IO.inspect() do
    with {:ok, tokens, "", _, _, _} <- parser.(messagestr) do
      map =
        tokens
        |> List.flatten()
        |> maybe_merge_sd_elements()
        |> Map.new()
        |> Map.merge(%{message_raw: messagestr})
        |> rename_fields()
        |> merge_structured_data()

      syslog_message = struct(Syslog.Message, map)
      {:ok, syslog_message}
    else
      err -> err
    end
  end

  defp merge_structured_data(%{sd_elements: sd_elements} = m) when length(sd_elements) > 0 do
    {{:sd_name, sd_name}, sd_params_vals} =
      sd_elements
      |> hd()
      |> List.pop_at(0)

    sd =
      sd_elements
      |> List.flatten()
      |> Enum.reject(fn
        {:sd_name, _} -> true
        _ -> false
      end)
      |> Enum.chunk_every(2)
      |> Enum.map(fn [param_name: k, param_value: v] ->
        {k, v}
      end)
      |> Map.new()

    Map.merge(m, %{data: sd, data_id: sd_name})
  end

  defp merge_structured_data(m), do: m

  defp rename_fields(map) do
    map
    |> Enum.map(fn {k, v} ->
      {case k do
         :proc_id -> :process_id
         :appname -> :app_name
         :hostname -> :host_name
         :msg_text -> :message
         _ -> k
       end, v}
    end)
    |> Map.new()
  end

  defp maybe_merge_sd_elements(tokens) do
    {sd_element_values, new_tokens} = Keyword.pop_values(tokens, :sd_element)

    if sd_element_values do
      Keyword.put(new_tokens, :sd_elements, sd_element_values)
    else
      tokens
    end
  end

  defparsec :sd_element, sd_element
  defparsec :sd_param, param_value
end

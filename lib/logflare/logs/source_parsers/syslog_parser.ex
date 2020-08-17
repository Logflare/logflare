defmodule Logflare.Logs.SyslogParser do
  import NimbleParsec
  import Logflare.Logs.SyslogParser.Helpers
  alias Logflare.Syslog

  defparsec(
    :do_parse,
    optional(
      byte_length()
      |> ignore(sp())
    )
    |> concat(priority())
    |> concat(version())
    |> ignore(sp())
    |> concat(maybe(timestamp()))
    |> ignore(sp())
    |> concat(maybe(hostname()))
    |> ignore(sp())
    |> concat(maybe(appname()))
    |> ignore(sp())
    |> concat(maybe(proc_id()))
    |> ignore(sp())
    |> concat(maybe(msg_id()))
    |> concat(
      ignore(sp())
      |> maybe(sd_element())
    )
    |> concat(
      optional(
        ignore(sp())
        |> message_text()
      )
    )
  )

  defparsec(
    :do_parse_heroku_dialect,
    byte_length()
    |> ignore(sp())
    |> concat(priority())
    |> concat(version())
    |> ignore(sp())
    |> concat(maybe(timestamp()))
    |> ignore(sp())
    |> concat(maybe(hostname()))
    |> ignore(sp())
    |> concat(maybe(appname()))
    |> ignore(sp())
    |> concat(maybe(proc_id()))
    |> ignore(sp())
    |> concat(maybe(msg_id()))
    |> concat(
      optional(
        ignore(sp())
        |> maybe(sd_element())
      )
    )
    |> concat(
      optional(
        ignore(sp())
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
    sd_names =
      sd_elements
      |> Enum.map(&hd/1)
      |> Enum.map(fn {:sd_name, sd_name} -> sd_name end)

    sd =
      sd_elements
      |> Enum.flat_map(fn [sd_name | rest] -> rest end)
      |> Enum.map(fn [param_name: k, param_value: v] ->
        {k, v}
      end)
      |> Map.new()

    Map.merge(m, %{data: sd, data_ids: sd_names})
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
end

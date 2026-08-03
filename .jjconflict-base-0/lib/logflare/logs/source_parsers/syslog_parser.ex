defmodule Logflare.Logs.SyslogParser do
  @moduledoc false
  import NimbleParsec
  import Logflare.Logs.SyslogParser.Helpers
  alias Logflare.Logs.SyslogMessage

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

  @doc """
  Parses incoming message string into Logflare.Syslog.Message struct.
  """
  def parse(messagestr, opts \\ []) when is_binary(messagestr) do
    parser =
      case opts[:dialect] do
        :heroku -> &do_parse_heroku_dialect/1
        nil -> &do_parse/1
      end

    messagestr = String.trim(messagestr)

    case parser.(messagestr) do
      {:ok, tokens, "", _, _, _} ->
        map =
          tokens
          |> List.flatten()
          |> merge_syslog_sd()
          |> merge_json()
          |> Map.new()
          |> Map.merge(%{message_raw: messagestr})
          |> rename_fields()

        syslog_message = struct(SyslogMessage, map)

        logfmt =
          case syslog_message.process_id do
            "router" ->
              parse_logfmt(syslog_message.message_text)

            "heroku-postgres" ->
              parse_logfmt(syslog_message.message_text)

            "heroku-redis" ->
              parse_logfmt(syslog_message.message_text)

            "heroku" <> _rest ->
              parse_logfmt(syslog_message.message_text)

            _ ->
              nil
          end

        syslog_message = Map.put(syslog_message, :logfmt, logfmt)

        {:ok, syslog_message}

      {:error, error, _, _, _, _} ->
        {:error, error}
    end
  end

  defp merge_json(tokens) when is_list(tokens) do
    case Keyword.pop_values(tokens, :msg_json) do
      {[msg_json], new_tokens} ->
        data = Map.put(msg_json, "id", "json")
        Keyword.update(new_tokens, :sd, [data], &[data | &1])

      _ ->
        tokens
    end
  end

  defp merge_syslog_sd(tokens) when is_list(tokens) do
    {sd_element_values, new_tokens} = Keyword.pop_values(tokens, :sd_element)

    if Enum.empty?(sd_element_values) do
      tokens
    else
      sd = build_sd(sd_element_values)

      new_tokens ++ [sd: sd]
    end
  end

  defp build_sd(sd_element_values) do
    sd_element_values
    |> Enum.map(fn sd_element ->
      Enum.reduce(sd_element, %{}, fn
        {:sd_name, sd_name}, acc -> Map.put(acc, "id", sd_name)
        [param_name: k, param_value: v], acc -> Map.put(acc, k, v)
      end)
    end)
  end

  defp rename_fields(map) do
    map
    |> Enum.map(fn {k, v} ->
      {case k do
         :proc_id -> :process_id
         :msg_text -> :message
         :msg_id -> :message_id
         _ -> k
       end, v}
    end)
    |> Map.new()
  end

  defp parse_logfmt(string) do
    case Logfmt.decode(string) do
      {:ok, data} -> data
      {:error, _} -> nil
    end
  end
end

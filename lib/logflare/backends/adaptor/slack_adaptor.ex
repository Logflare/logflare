defmodule Logflare.Backends.Adaptor.SlackAdaptor do
  @moduledoc false
  use LogflareWeb, :routes

  alias __MODULE__.Client

  alias Logflare.Sources.Source
  alias Logflare.Alerting.AlertQuery

  @doc """
  Sends a given payload to slack.

  Returns Tesla response.
  """
  @spec send_message(String.t() | AlertQuery.t(), [map()]) :: Tesla.Env.result()
  def send_message(%AlertQuery{id: id, name: name, slack_hook_url: hook_url}, payload) do
    rows_text =
      case Enum.count(payload) do
        0 -> ""
        1 -> ", 1 row"
        n -> ", #{n} rows"
      end

    view_url = url(~p"/alerts/#{id}")
    context = "🔊 *#{name}*#{rows_text} | #{view_url}"

    body =
      payload
      |> to_body(
        button_link: %{
          markdown_text: context,
          url: view_url,
          text: "Manage"
        }
      )

    Client.send(hook_url, body)
  end

  def send_message(url, payload) when is_binary(url) do
    body = payload |> to_body()
    Client.send(url, body)
  end

  @spec send_message(Source.t(), [map()], pos_integer()) :: Tesla.Env.result()
  def send_message(%Source{slack_hook_url: url} = source, log_events, rate) do
    body = build_message(source, log_events, rate)

    Logger.metadata(slackhook_request: %{url: url, body: inspect(body)}, user_id: source.user_id, system_source: source.system_source)
    Client.send(url, body)
  end

  def build_message(%Source{id: id, name: source_name}, log_events, rate) do
    events =
      log_events
      |> Enum.map(fn le ->
        {:ok, dt} = DateTime.from_unix(le.body["timestamp"], :microsecond)
        %{DateTime.to_string(dt) => le.body["event_message"]}
      end)

    source_link = url(~p"/sources/#{id}")

    to_body(events,
      button_link: %{
        markdown_text: "*#{rate} new event(s)* for `#{source_name}`",
        text: "View events",
        url: source_link
      }
    )
  end

  @doc """
  Formats lists of maps into expected slack payload.
  The payload will be converted to markdown.

  ### Example

    iex> %{blocks: [_]} = to_body([%{"some" => "key"}])
    iex> %{blocks: [_, _]} = to_body([%{"some" => "key"}], context: "some context")

  """
  @spec to_body([map()]) :: map()
  def to_body(results, opts \\ []) when is_list(results) do
    context = Keyword.get(opts, :context)
    button_link = Keyword.get(opts, :button_link)

    %{
      blocks:
        build_context_blocks(context) ++
          build_button_link_blocks(button_link) ++
          build_rich_text_blocks(results)
    }
  end

  defp build_button_link_blocks(nil), do: []

  defp build_button_link_blocks(button_link) do
    [
      %{
        type: "section",
        text: %{
          type: "mrkdwn",
          text: button_link.markdown_text
        },
        accessory: %{
          type: "button",
          text: %{type: "plain_text", text: button_link.text},
          url: button_link.url,
          style: "primary"
        }
      }
    ]
  end

  defp build_context_blocks(nil), do: []

  defp build_context_blocks(context) do
    [
      %{
        type: "context",
        elements: [%{type: "mrkdwn", text: context}]
      }
    ]
  end

  defp build_rich_text_blocks(results) do
    Enum.map(results, fn row ->
      %{
        type: "rich_text",
        elements: [
          %{
            type: "rich_text_preformatted",
            elements: to_rich_text_preformatted(row)
          }
        ]
      }
    end)
  end

  @doc """
  Converts elixir terms into slack-compatible markdown.
  Accepted terms:
  - list of maps
  - map

  Text will have a colon separating the key-value pair.
  Multi-line strings will be separated with a line break
  Keys will be sorted alphabetically with a line break in between each key
  If link is present in the results, it will be conveted into a url
  """
  @spec to_rich_text_preformatted([map()] | map()) :: [map()]

  def to_rich_text_preformatted(%{} = row) do
    row
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map_intersperse([line_break()], fn {k, v} ->
      v_str = stringify(v)

      cond do
        is_number(v) and String.length(v_str) == 16 ->
          # convert to timestamp
          {:ok, dt} = DateTime.from_unix(v, :microsecond)

          [text("#{k}:"), space(), text(DateTime.to_string(dt))]

        String.starts_with?(v_str, ["http://", "https://"]) ->
          [text("#{k}:"), space(), link(v_str)]

        v_str =~ "\n" ->
          [text("#{k}:"), line_break(), text(v_str)]

        true ->
          [text("#{k}:"), space(), text(v_str)]
      end
    end)
    |> List.flatten()
  end

  defp link(v) do
    %{type: "link", url: v}
  end

  defp space do
    %{type: "text", text: " "}
  end

  defp line_break do
    %{type: "text", text: "\n"}
  end

  defp text(v) do
    %{type: "text", text: v}
  end

  defp stringify(nil), do: nil
  defp stringify(v) when is_binary(v), do: v

  defp stringify(v) when is_integer(v) do
    Integer.to_string(v)
  end

  defp stringify(v) when is_float(v) do
    Float.to_string(v)
  end

  defp stringify(%{} = v), do: Jason.encode!(v)
  defp stringify(v), do: inspect(v)
end

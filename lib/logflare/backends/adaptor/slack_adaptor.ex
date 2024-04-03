defmodule Logflare.Backends.Adaptor.SlackAdaptor do
  @moduledoc false
  alias __MODULE__.Client

  alias Logflare.Alerting.AlertQuery

  @doc """
  Sends a given payload to slack.

  Returns Tesla response.
  """
  @spec send_message(String.t() | AlertQuery.t(), [map()]) :: Tesla.Env.result()
  def send_message(%AlertQuery{name: name, slack_hook_url: url}, payload) do
    rows_text =
      case Enum.count(payload) do
        0 -> ""
        1 -> ", 1 row"
        n -> ", #{n} rows"
      end

    context = "🔊 *#{name}*#{rows_text}"

    body =
      payload
      |> to_body(context: context)

    Client.send(url, body)
  end

  def send_message(url, payload) when is_binary(url) do
    body = payload |> to_body()
    Client.send(url, body)
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

    %{
      blocks:
        ([
           if(context != nil,
             do: %{
               type: "context",
               elements: [%{type: "mrkdwn", text: context}]
             }
           )
         ] ++
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
           end))
        |> Enum.filter(&(&1 != nil))
    }
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
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map(fn {k, v} ->
      cond do
        String.starts_with?(v, ["http://", "https://"]) ->
          [text("#{k}:"), space(), link(v)]

        v =~ "\n" ->
          [text("#{k}:"), line_break(), text(v)]

        true ->
          [text("#{k}:"), space(), text(v)]
      end
    end)
    |> Enum.intersperse([line_break()])
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
end

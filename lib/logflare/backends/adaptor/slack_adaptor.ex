defmodule Logflare.Backends.Adaptor.SlackAdaptor do
  @moduledoc false
  alias __MODULE__.Client

  @doc """
  Sends a given payload to slack.

  Returns Tesla response.
  """
  @spec send_message(String.t(), [map()]) :: Tesla.Env.result()
  def send_message(url, payload) do
    body = to_body(payload)
    Client.send(url, body)
  end

  @doc """
  Formats lists of maps into expected slack payload.
  The payload will be converted to markdown.

  ### Example

    iex> %{blocks: [block]} = to_body([%{"some" => "key"}])
    iex> get_in(block, [:text, :text])
    ["•some: key"]

  """
  @spec to_body([map()]) :: map()
  def to_body(results) when is_list(results) do
    %{
      blocks: [
        %{type: "section", text: %{type: "mrkdwn", text: to_markdown(results)}}
      ]
    }
  end

  @doc """
  Converts elixir terms into slack-compatible markdown.
  Accepted terms:
  - list of maps
  - map

  ### Example
  The text will be prefixed with a bullet point, with a colon separating the key-value pair.
    iex> to_markdown(%{"test"=> "test"})
    "•test: test"

  Keys within the map will be placed in an indentedrow below, sorted by key alphabetically.
  Indentation uses 4 spaces.
    iex> to_markdown(%{a: "test", b: "test"})
    "•a: test\r    b: test"

  Muliple objects will be converted into lists of strings
    iex> to_markdown([%{one: "test"}, %{two: "test"}])
    ["•one: test", "•two: test"]

  """
  @spec to_markdown([map()] | map()) :: [String.t()] | String.t()
  def to_markdown(rows) when is_list(rows), do: Enum.map(rows, &to_markdown/1)

  def to_markdown(%{} = row) do
    bullet = "•"
    indent = "    "

    text =
      row
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.map(fn {k, v} -> "#{k}: #{v}" end)
      |> Enum.join("\r" <> indent)

    bullet <> text
  end
end

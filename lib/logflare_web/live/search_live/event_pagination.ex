defmodule LogflareWeb.SearchLive.EventPagination do
  @moduledoc false

  alias Logflare.Logs.EventPage

  @type cursor :: EventPage.cursor()
  @type range_extension :: String.t()
  @type button_state :: :hidden | :ready | :disabled | :loading
  @type button :: %{state: button_state(), cursor: cursor() | nil, label: String.t()}
  @type buttons :: %{previous: button(), next: button()}

  @enforce_keys []
  defstruct range_extension: nil, loading_intent: nil, requested_at: nil, window_seconds: nil

  @type t :: %__MODULE__{
          range_extension: range_extension() | nil,
          loading_intent: EventPage.direction() | nil,
          requested_at: integer() | nil,
          window_seconds: pos_integer() | nil
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec mark_range_extension(t(), String.t()) :: t()
  def mark_range_extension(pagination, querystring) do
    %{pagination | range_extension: querystring}
  end

  @spec clear_range_extension(t()) :: t()
  def clear_range_extension(pagination), do: %{pagination | range_extension: nil}

  @doc """
  Sets the window that every page request of this search moves by.

  The button label, the page query, the cursor shift and the range extension all read this
  one value, so a click moves exactly as far as its button says.
  """
  @spec put_window(t(), pos_integer()) :: t()
  def put_window(pagination, window_seconds)
      when is_integer(window_seconds) and window_seconds > 0,
      do: %{pagination | window_seconds: window_seconds}

  @doc """
  Marks a page request as in flight so its button keeps spinning.

  `phx-click-loading` only covers the event round trip, which ends as soon as
  `load_events` hands the query to the executor. The query itself finishes much
  later, so the button state has to come from the server.

  `requested_at` is the request time in microseconds. An empty "next" page never moves
  its cursor past it.
  """
  @spec mark_loading(t(), EventPage.direction(), integer()) :: t()
  def mark_loading(pagination, intent, requested_at)
      when intent in [:previous, :next] and is_integer(requested_at),
      do: %{pagination | loading_intent: intent, requested_at: requested_at}

  @spec clear_loading(t()) :: t()
  def clear_loading(pagination), do: %{pagination | loading_intent: nil, requested_at: nil}

  @doc """
  Returns whether a page request with this intent is in flight.

  A page result or page error with any other intent belongs to a request that a new search
  already replaced.
  """
  @spec loading?(t(), EventPage.direction()) :: boolean()
  def loading?(%__MODULE__{loading_intent: intent}, intent) when not is_nil(intent), do: true
  def loading?(%__MODULE__{}, _intent), do: false

  @spec buttons(t(), keyword()) :: buttons()
  def buttons(pagination, options) do
    tailing? = Keyword.fetch!(options, :tailing?)
    cursors = Keyword.fetch!(options, :cursors)
    busy? = Keyword.fetch!(options, :loading?) or not is_nil(pagination.loading_intent)

    %{
      previous: button(pagination, :previous, cursors.previous, tailing?, busy?),
      next: button(pagination, :next, cursors.next, tailing?, busy?)
    }
  end

  @doc """
  Label for a pagination button, naming the window one click moves by.

  A page request scans that window and widens the query's timestamp range by it, so the
  button says exactly how far the next click travels.
  """
  @spec label(pos_integer() | nil, String.t()) :: String.t()
  def label(nil, _sign), do: "Load more"

  def label(window_seconds, sign) when is_integer(window_seconds) and window_seconds > 0 do
    {amount, unit} = humanize(window_seconds)
    "Load more (#{sign}#{amount} #{unit})"
  end

  defp humanize(seconds) when seconds < 60, do: {seconds, pluralize(seconds, "second")}

  defp humanize(seconds) when seconds < 3_600 do
    amount = div(seconds, 60)
    {amount, pluralize(amount, "minute")}
  end

  defp humanize(seconds) when seconds < 86_400 do
    amount = div(seconds, 3_600)
    {amount, pluralize(amount, "hour")}
  end

  defp humanize(seconds) do
    amount = div(seconds, 86_400)
    {amount, pluralize(amount, "day")}
  end

  defp pluralize(1, unit), do: unit
  defp pluralize(_amount, unit), do: unit <> "s"

  defp button(pagination, intent, cursor, tailing?, busy?) do
    state =
      cursor
      |> button_state(tailing?, busy?)
      |> apply_loading(pagination.loading_intent == intent)

    %{state: state, cursor: cursor, label: label(pagination.window_seconds, sign(intent))}
  end

  defp sign(:previous), do: "-"
  defp sign(:next), do: "+"

  defp apply_loading(:hidden, _loading?), do: :hidden
  defp apply_loading(_state, true), do: :loading
  defp apply_loading(state, _loading?), do: state

  defp button_state(_cursor, true, _busy?), do: :hidden
  defp button_state(nil, _tailing?, _busy?), do: :hidden
  defp button_state(_cursor, _tailing?, true), do: :disabled
  defp button_state(_cursor, _tailing?, _busy?), do: :ready
end

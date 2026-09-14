defmodule LogflareWeb.SearchLive.EventPagination do
  @moduledoc false

  alias Logflare.Logs.EventPage

  @type cursor :: EventPage.cursor()
  @type range_extension :: String.t()
  @type button_state :: :hidden | :ready | :disabled | :loading
  @type button :: %{state: button_state(), cursor: cursor() | nil, label: String.t()}
  @type buttons :: %{previous: button(), next: button()}

  @enforce_keys []
  defstruct next_exhausted?: false, range_extension: nil, loading_intent: nil

  @type t :: %__MODULE__{
          next_exhausted?: boolean(),
          range_extension: range_extension() | nil,
          loading_intent: EventPage.direction() | nil
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec complete_initial(t(), EventPage.t()) :: t()
  def complete_initial(pagination, _event_page) do
    %{pagination | next_exhausted?: false}
  end

  @spec complete_page(t(), EventPage.t(), EventPage.direction()) :: t()
  def complete_page(pagination, _event_page, :previous), do: pagination

  def complete_page(pagination, event_page, :next) do
    %{pagination | next_exhausted?: not event_page.has_more?}
  end

  @spec complete_tail(t(), EventPage.t()) :: t()
  def complete_tail(pagination, %EventPage{next_cursor: nil}), do: pagination

  def complete_tail(pagination, %EventPage{}), do: %{pagination | next_exhausted?: false}

  @spec mark_range_extension(t(), String.t()) :: t()
  def mark_range_extension(pagination, querystring) do
    %{pagination | range_extension: querystring}
  end

  @spec clear_range_extension(t()) :: t()
  def clear_range_extension(pagination), do: %{pagination | range_extension: nil}

  @doc """
  Marks a page request as in flight so its button keeps spinning.

  `phx-click-loading` only covers the event round trip, which ends as soon as
  `load_events` hands the query to the executor. The query itself finishes much
  later, so the button state has to come from the server.
  """
  @spec mark_loading(t(), EventPage.direction()) :: t()
  def mark_loading(pagination, intent) when intent in [:previous, :next],
    do: %{pagination | loading_intent: intent}

  @spec clear_loading(t()) :: t()
  def clear_loading(pagination), do: %{pagination | loading_intent: nil}

  @spec buttons(t(), keyword()) :: buttons()
  def buttons(pagination, options) do
    tailing? = Keyword.fetch!(options, :tailing?)
    loading? = Keyword.fetch!(options, :loading?)
    next_available? = Keyword.fetch!(options, :next_available?)
    cursors = Keyword.fetch!(options, :cursors)

    window_seconds = Keyword.get(options, :window_seconds)

    %{
      previous: %{
        state:
          cursors.previous
          |> previous_button(tailing?, loading?)
          |> apply_loading(pagination.loading_intent == :previous),
        cursor: cursors.previous,
        label: label(window_seconds, "-")
      },
      next: %{
        state:
          pagination
          |> next_button(tailing?, loading?, next_available?)
          |> apply_loading(pagination.loading_intent == :next),
        cursor: cursors.next,
        label: label(window_seconds, "+")
      }
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

  defp apply_loading(:hidden, _loading?), do: :hidden
  defp apply_loading(_state, true), do: :loading
  defp apply_loading(state, _loading?), do: state

  defp previous_button(_cursor, true, _loading?), do: :hidden
  defp previous_button(nil, _tailing?, _loading?), do: :hidden
  defp previous_button(_cursor, _tailing?, true), do: :disabled

  defp previous_button(_cursor, _tailing?, _loading?), do: :ready

  defp next_button(_pagination, true, _loading?, _next_available?), do: :hidden
  defp next_button(_pagination, _tailing?, _loading?, false), do: :hidden

  defp next_button(%__MODULE__{next_exhausted?: true}, _tailing?, _loading?, _next_available?),
    do: :hidden

  defp next_button(_pagination, _tailing?, true, _next_available?), do: :disabled

  defp next_button(_pagination, _tailing?, _loading?, _next_available?), do: :ready
end

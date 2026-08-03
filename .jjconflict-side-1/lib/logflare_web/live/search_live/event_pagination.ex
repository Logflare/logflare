defmodule LogflareWeb.SearchLive.EventPagination do
  @moduledoc false

  alias Logflare.Logs.EventPage

  @type cursor :: EventPage.cursor()
  @type range_extension :: String.t()
  @type button_state :: :hidden | :ready | :disabled
  @type button :: %{state: button_state(), cursor: cursor() | nil}
  @type buttons :: %{previous: button(), next: button()}

  @enforce_keys []
  defstruct next_exhausted?: false, range_extension: nil

  @type t :: %__MODULE__{
          next_exhausted?: boolean(),
          range_extension: range_extension() | nil
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec complete_initial(t(), EventPage.t()) :: t()
  def complete_initial(pagination, event_page) do
    %{pagination | next_exhausted?: not event_page.has_more?}
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

  @spec buttons(t(), keyword()) :: buttons()
  def buttons(pagination, options) do
    tailing? = Keyword.fetch!(options, :tailing?)
    loading? = Keyword.fetch!(options, :loading?)
    next_available? = Keyword.fetch!(options, :next_available?)
    cursors = Keyword.fetch!(options, :cursors)

    %{
      previous: %{
        state: previous_button(cursors.previous, tailing?, loading?),
        cursor: cursors.previous
      },
      next: %{
        state: next_button(pagination, tailing?, loading?, next_available?),
        cursor: cursors.next
      }
    }
  end

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

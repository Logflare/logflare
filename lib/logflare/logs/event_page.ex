defmodule Logflare.Logs.EventPage do
  @moduledoc false

  use TypedStruct

  alias Logflare.LogEvent

  @type cursor :: %{timestamp: integer(), id: String.t()}
  @type direction :: :previous | :next
  @type intent :: :initial | :previous | :next
  @type request :: %{
          required(:intent) => intent(),
          required(:cursor) => cursor() | nil,
          optional(:window_seconds) => pos_integer(),
          optional(:requested_at) => integer()
        }

  typedstruct do
    field :rows, [LogEvent.t()], enforce: true
    field :request, request(), enforce: true
    field :cursor, cursor() | nil, enforce: true
    field :next_cursor, cursor() | nil, default: nil
    field :events, :any, default: nil
  end

  @spec direction(intent()) :: direction()
  def direction(:next), do: :next
  def direction(:previous), do: :previous
  def direction(:initial), do: :previous

  @doc """
  Returns whether an event-page request has a valid intent, cursor and window.

  The window is the number of seconds the page query scans from the cursor.

  An intent can be one of:
  * `:next` - fetching the next page of newer events after the curosr
  * `:previous` - fetch a page of older events before the cursor
  * `:initial` - no cursor, used for the first search by Search LiveView.
  """
  @spec valid_request?(term(), term(), term()) :: boolean()
  def valid_request?(intent, cursor, window_seconds)
      when intent in [:previous, :next] and is_integer(window_seconds) and window_seconds > 0,
      do: valid_cursor?(cursor)

  def valid_request?(_intent, _cursor, _window_seconds), do: false

  @doc """
  Validate the cursor for the page query has a timestamp and id.

  The cursor timestamp is used as the beginning of the page request. The id is
  used to deduplicate events with the same timestamp.
  """
  @spec valid_cursor?(term()) :: boolean()
  def valid_cursor?(%{timestamp: timestamp, id: id}) when is_integer(timestamp) and is_binary(id),
    do: true

  def valid_cursor?(_cursor), do: false
end

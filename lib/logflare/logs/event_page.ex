defmodule Logflare.Logs.EventPage do
  @moduledoc false

  use TypedStruct

  alias Logflare.LogEvent

  @type boundary :: integer() | DateTime.t() | NaiveDateTime.t()
  @type cursor :: %{timestamp: integer(), id: String.t()}
  @type direction :: :previous | :next
  @type intent :: :initial | :previous | :next
  @type request :: %{
          intent: intent(),
          boundary: boundary() | nil,
          cursor: cursor() | nil
        }

  typedstruct do
    field :rows, [LogEvent.t()], enforce: true
    field :request, request(), enforce: true
    field :cursor, cursor() | nil, enforce: true
    field :next_cursor, cursor() | nil, default: nil
    field :has_more?, boolean(), enforce: true
    field :events, :any, default: nil
  end

  @spec direction(intent()) :: direction()
  def direction(:next), do: :next
  def direction(_intent), do: :previous

  @spec boundary(intent(), %{min: boundary(), max: boundary()} | nil) :: boundary() | nil
  def boundary(:next, %{max: max}), do: max
  def boundary(_intent, _bounds), do: nil

  @spec valid_request?(term(), term(), boundary() | nil) :: boolean()
  def valid_request?(:previous, cursor, _boundary), do: valid_cursor?(cursor)

  def valid_request?(:next, cursor, boundary) do
    not is_nil(boundary) or valid_cursor?(cursor)
  end

  def valid_request?(_intent, _cursor, _boundary), do: false

  @spec valid_cursor?(term()) :: boolean()
  def valid_cursor?(%{timestamp: timestamp, id: id}) when is_integer(timestamp) and is_binary(id),
    do: true

  def valid_cursor?(_cursor), do: false
end

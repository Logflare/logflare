defmodule Logflare.Logs.EventPage do
  @moduledoc false

  use TypedStruct

  alias Logflare.LogEvent

  @type cursor :: %{timestamp: integer(), id: String.t()}
  @type direction :: :previous | :next
  @type intent :: :initial | :previous | :next
  @type request :: %{
          intent: intent(),
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
  def direction(:previous), do: :previous
  def direction(:initial), do: :previous



  def valid_request?(_intent, _cursor), do: false


  @spec valid_cursor?(term()) :: boolean()
  def valid_cursor?(%{timestamp: timestamp, id: id}) when is_integer(timestamp) and is_binary(id),
    do: true

  def valid_cursor?(_cursor), do: false
end

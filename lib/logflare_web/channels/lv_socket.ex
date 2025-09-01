defmodule Logflare.LiveView.Socket do
  @moduledoc """
  The LiveView socket for Phoenix Endpoints.
  """
  use Phoenix.Socket, log: false

  alias Phoenix.LiveView

  require Logger

  @derive {Inspect,
           only: [:id, :endpoint, :router, :view, :parent_pid, :root_pid, :assigns, :changed]}

  defstruct id: nil,
            endpoint: nil,
            view: nil,
            root_view: nil,
            parent_pid: nil,
            root_pid: nil,
            router: nil,
            assigns: %{},
            changed: %{},
            private: %{},
            fingerprints: LiveView.Diff.new_fingerprints(),
            redirected: nil,
            host_uri: nil,
            connected?: false

  @type assigns :: map | LiveView.Socket.AssignsNotInSocket.t()
  @type fingerprints :: {nil, map} | {binary, map}

  @type t :: %__MODULE__{
          id: binary(),
          endpoint: module(),
          view: module(),
          root_view: module(),
          parent_pid: nil | pid(),
          root_pid: pid(),
          router: module(),
          assigns: assigns,
          changed: map(),
          private: map(),
          fingerprints: fingerprints,
          redirected: nil | tuple(),
          host_uri: URI.t(),
          connected?: boolean()
        }

  channel "lv:*", LiveView.Channel

  @doc """
  Connects the Phoenix.Socket for a LiveView client.
  """
  @impl Phoenix.Socket
  def connect(_params, %Phoenix.Socket{} = socket, connect_info) do
    {:ok, put_in(socket.private[:connect_info], connect_info)}
  end

  @doc """
  Identifies the Phoenix.Socket for a LiveView client.
  """
  @impl Phoenix.Socket
  def id(socket), do: socket.private.connect_info[:session]["live_socket_id"]
end

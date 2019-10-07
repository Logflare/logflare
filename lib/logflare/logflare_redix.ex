defmodule Logflare.Redix do
  @default_conn :logflare_redix
  def child_spec(_args) do
    # Specs for the Redix connections.
    children = [
      Supervisor.child_spec({Redix, name: @default_conn, host: "localhost", port: 6379},
        id: {Redix, 0}
      )
    ]

    # Spec for the supervisor that will supervise the Redix connections.
    %{
      id: RedixSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def increment(key) do
    Redix.command(@default_conn, ["INCR", key])
  end

  def get(key) do
    Redix.command(@default_conn, ["GET", key])
  end

  def set(key, value) do
    Redix.command(@default_conn, ["SET", key, value])
  end

  def command(args) when is_list(args) do
    Redix.command(@default_conn, args)
  end
end

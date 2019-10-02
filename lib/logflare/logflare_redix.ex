defmodule Logflare.Redix do
  def child_spec(_args) do
    # Specs for the Redix connections.
    children = [
      Supervisor.child_spec({Redix, name: :redix, host: "localhost", port: 6379}, id: {Redix, 0})
    ]

    # Spec for the supervisor that will supervise the Redix connections.
    %{
      id: RedixSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end
end

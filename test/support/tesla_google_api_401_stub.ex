defmodule Logflare.Tesla.GoogleApi401Stub do
  @behaviour Tesla.Adapter

  @body JSON.encode!(%{
          "error" => %{
            "code" => 401,
            "message" => "Request had invalid authentication credentials.",
            "status" => "UNAUTHENTICATED"
          }
        })

  @impl true
  def call(env, _opts) do
    {:ok, %{env | status: 401, headers: [{"content-type", "application/json"}], body: @body}}
  end
end

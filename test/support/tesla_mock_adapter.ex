defmodule Logflare.Tesla.MockAdapter do
  @behaviour Tesla.Adapter

  @impl true
  def call(env, opts) do
    opts[:call].(env)
  end

  def replace(client, function) do
    Tesla.client(Tesla.Client.middleware(client), {Logflare.Tesla.MockAdapter, call: function})
  end
end

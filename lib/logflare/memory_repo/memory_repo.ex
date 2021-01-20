defmodule Logflare.MemoryRepo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Mnesia

  alias Logflare.EctoSchemaReflection

  @config Application.get_env(:logflare, Logflare.MemoryRepo)

  def config(:changefeed_subscriptions) do
    concat_modules(@config[:changefeed_subscriptions])
  end

  def config(:tables) do
    concat_modules(@config[:tables])
  end

  def concat_modules(kw) do
    for chfd <- kw do
      case chfd do
        {c, opts} ->
          {Module.concat(Logflare, c), opts}

        c when is_atom(c) ->
          Module.concat(Logflare, c)
      end
    end
  end

  def reset_mnesia() do
    :stopped = :mnesia.stop()
    :ok = :mnesia.delete_schema([Node.self()])
    :ok = :mnesia.create_schema([Node.self()])
    :ok = :mnesia.start()
  end
end

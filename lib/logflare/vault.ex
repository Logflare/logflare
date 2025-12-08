defmodule Logflare.Vault do
  @doc """
  GenServer needed for Cloak.
  It handles secrets migration for key rolling at startup.

  To run the migration at runtime, use the following:
  ```elixir
  iex> Logflare.Vault.do_migrate()
  ```
  An old encryption key should be present for the migration.

  """
  use Cloak.Vault, otp_app: :logflare

  alias Cloak.Ecto.Migrator
  require Logger
  import Ecto.Query

  @schema_fields %{
    Logflare.Backends.Backend => [:config_encrypted]
  }

  @impl GenServer
  def init(config) do
    if Application.get_env(:logflare, :env) == :test do
      # make ets table public
      :ets.new(@table_name, [:named_table, :public])
    end

    fallback_key = Application.get_env(:logflare, :encryption_key_fallback) |> maybe_decode!()

    default_key =
      Application.get_env(:logflare, :encryption_key_default) |> maybe_decode!() || fallback_key

    retired_key = Application.get_env(:logflare, :encryption_key_retired) |> maybe_decode!()

    fallback_cipher = {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1." <> hash(fallback_key), key: fallback_key}
    ciphers =
      [
        default:
          {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1." <> hash(default_key), key: default_key},
        retired:
          if(retired_key != nil,
            do: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1" <> hash(retired_key), key: retired_key},
            else: fallback_cipher
          ),
        fallback: fallback_cipher
      ]

    config = Keyword.put(config, :ciphers, ciphers)

    Task.start_link(fn ->
      # wait for genserver config to be saved, see https://github.com/danielberkompas/cloak/blob/v1.1.4/lib/cloak/vault.ex#L186
      :timer.sleep(1_000)
      maybe_migrate(ciphers)
    end)

    {:ok, config}
  end

  def maybe_migrate(ciphers) when is_list(ciphers) do
    ciphers_map = Map.new(ciphers)
    if should_migrate?(ciphers_map) do
      Logger.info("Migrating schemas to provided :default key")
      for {schema, _fields} <- @schema_fields do
        Migrator.migrate(Logflare.Repo, schema) |> dbg
      end
      Logger.info("Encryption migration complete")
      :ok
    else
      Logger.info("No encryption migration required")
      :noop
    end
  end


  # all values are the same
  defp should_migrate?(%{fallback: fallback_cipher, default: default_cipher, retired: retired_cipher}) when fallback_cipher == default_cipher and default_cipher == retired_cipher, do: false
  defp should_migrate?(%{ default: default_cipher, retired: retired_cipher}) when default_cipher == retired_cipher, do: false
  defp should_migrate?(ciphers_map) do
    dbg(ciphers_map.default == ciphers_map.retired)
    for {schema, fields} <- @schema_fields do
      should_migrate_schema?(schema, fields)
    end
    |> Enum.any?(fn val -> val == true end)
  end
  defp should_migrate_schema?(schema, fields) do
    table = struct(schema).__meta__.source
    with %{} = data <- Logflare.Repo.one(from b in table, select: ^fields, limit: 1)  do
      Enum.any?(fields, fn field ->
        raw = Map.get(data, field)
        decrypted = Logflare.Vault.decrypt!(raw)

        # only migrate if the data is encrypted with the retired key and not the default key
        dbg(raw)
        encrypted_with_retired = Logflare.Vault.encrypt!(decrypted, :retired) |> dbg()
        encrypted_with_default = Logflare.Vault.encrypt!(decrypted, :default) |> dbg()
        encrypted_with_fallback = Logflare.Vault.encrypt!(decrypted, :fallback) |> dbg()
        (raw != encrypted_with_default) |> dbg()
      end)
    else
      _ -> false
    end
  end

  # helper for loading keys
  defp maybe_decode!(nil), do: nil
  defp maybe_decode!(str), do: Base.decode64!(str)

  # used to hash the tag based on the key, as cloak uses the tag to determine cipher to use.
  defp hash(key) do
    :sha256 |> :crypto.hash(key) |> Base.encode64()
  end

  # helper for tests
  def get_config do
    Cloak.Vault.read_config(@table_name)
  end

  # helper for tests
  def save_config(config) do
    Cloak.Vault.save_config(@table_name, config)
  end

  # helper for tests
  def get_cipher(key) do
    key = key |> maybe_decode!()
    {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1" <> hash(key), key: key}
  end
end

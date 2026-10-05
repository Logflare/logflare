defmodule Logflare.Repo.Migrations.AddEncryptedConfigFieldForBackendsTable do
  use Ecto.Migration
  import Ecto.Query
  alias Logflare.Ecto.EncryptedMap

  def up do
    alter table(:backends) do
      add :config_encrypted, :binary
    end

    flush()
    {:ok, pid} = Logflare.Vault.start_link()

    # copy configs over
    repo().all(from b in "backends", select: [:id, :config])
    |> Enum.each(fn %{id: id} = backend ->
      {:ok, config_encrypted} = EncryptedMap.dump(backend.config)

      from(b in "backends",
        where: b.id == ^id,
        update: [set: [config_encrypted: ^config_encrypted]]
      )
      |> repo().update_all([])
    end)

    # stop the vault
    Process.unlink(pid)
    Process.exit(pid, :kill)
    :timer.sleep(100)
  end

  def down do
    alter table(:backends) do
      remove(:config_encrypted)
    end
  end
end

defmodule Logflare.Repo.Migrations.AddEncryptedConfigFieldForBackendsTable do
  use Ecto.Migration
  alias Logflare.Repo
  import Ecto.Query
  alias Logflare.Ecto.EncryptedMap

  def up do
    alter table(:backends) do
      add :config_encrypted, :binary
    end

    flush()

    # copy configs over
    Repo.all(from b in "backends", select: [:id, :config])
    |> Enum.each(fn %{id: id} = backend ->
      {:ok, config_encrypted} = EncryptedMap.cast(backend.config)

      from(b in "backends",
        where: b.id == ^id,
        update: [set: [config_encrypted: ^config_encrypted]]
      )
      |> Logflare.Repo.update_all([])
    end)
  end

  def down do
    alter table(:backends) do
      remove(:config_encrypted)
    end
  end
end

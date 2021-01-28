defmodule Logflare.RepoWithCache do
  alias Logflare.{Repo, MemoryRepo}
  alias Logflare.Changefeeds
  use Logflare.DeriveVirtualDecorator
  import Logflare.EctoDerived, only: [merge_virtual: 1]

  def update(changeset, opts \\ []) do
    with {:ok, updated} <- Repo.update(changeset, opts) do
      {:ok, mem} =
        MemoryRepo.update(
          changeset,
          opts
        )

      :ok = Changefeeds.maybe_insert_virtual(mem)

      {:ok, updated}
    else
      errtup -> errtup
    end
  end

  def update_all(queryable, updates, opts \\ []) do
    with {:ok, updated} <- Repo.update_all(queryable, updates, opts) do
      {:ok, mem} =
        MemoryRepo.update_all(
          queryable,
          updates,
          opts
        )

      :ok = Changefeeds.maybe_insert_virtual(mem)

      {:ok, updated}
    else
      errtup -> errtup
    end
  end

  def delete(struct_or_changeset, opts \\ []) do
    with {:ok, deleted} <- Repo.delete(struct_or_changeset, opts) do
      {:ok, mem} =
        MemoryRepo.delete(
          struct_or_changeset,
          opts
        )

      :ok = Changefeeds.maybe_delete_virtual(mem)

      {:ok, deleted}
    else
      errtup -> errtup
    end
  end

  def delete_all(queryable, opts) do
    with {:ok, deleted} <- Repo.delete_all(queryable, opts) do
      {:ok, mem} =
        MemoryRepo.delete_all(
          queryable,
          opts
        )

      :ok = Changefeeds.maybe_delete_virtual(queryable)

      {:ok, deleted}
    else
      errtup -> errtup
    end
  end

  def insert(struct_or_changeset, opts \\ []) do
    with {:ok, inserted} <- Repo.insert(struct_or_changeset, opts) do
      {:ok, mem} =
        MemoryRepo.insert(
          Changefeeds.replace_assocs_with_nils(inserted),
          Keyword.merge(opts, on_conflict: :replace_all, conflict_target: :id)
        )

      :ok = Changefeeds.maybe_insert_virtual(mem)

      {:ok, inserted}
    else
      errtup -> errtup
    end
  end

  def preload(structs_or_struct_or_nil, preloads, opts \\ []) do
    with structs_or_struct_or_nil <- MemoryRepo.preload(structs_or_struct_or_nil, preloads, opts) do
      if is_list(preloads) do
        for {k, _} <- preloads, reduce: structs_or_struct_or_nil do
          x -> merge_virtual_for_preload(x, k)
        end
      else
        merge_virtual_for_preload(structs_or_struct_or_nil, preloads)
      end
    else
      errtup -> errtup
    end
  end

  def merge_virtual_for_preload(result, preload_field) when is_atom(preload_field) do
    if is_struct(result) do
      new_assoc =
        result
        |> Map.get(preload_field)
        |> merge_virtual()

      %{result | preload_field => new_assoc}
    else
      result
    end
  end

  def insert_all(schema_or_source, entries, opts) do
    with {:ok, inserted} <- Repo.insert_all(schema_or_source, entries, opts) do
      {:ok, mem} = MemoryRepo.insert_all(schema_or_source, entries, opts)

      :ok = Changefeeds.maybe_insert_virtual(mem)

      {:ok, inserted}
    else
      errtup -> errtup
    end
  end

  @decorate update_virtual_fields()
  defdelegate one(queryable), to: MemoryRepo
  defdelegate one(queryable, opts), to: MemoryRepo

  @decorate update_virtual_fields()
  defdelegate all(queryable), to: MemoryRepo
  defdelegate all(queryable, opts), to: MemoryRepo

  @decorate update_virtual_fields()
  defdelegate get_by(queryable, clauses), to: MemoryRepo
  defdelegate get_by(queryable, clauses, opts), to: MemoryRepo

  @decorate update_virtual_fields()
  defdelegate get(queryable, id), to: MemoryRepo
  defdelegate get(queryable, id, opts), to: MemoryRepo

  @decorate update_virtual_fields()
  defdelegate get!(queryable, id), to: MemoryRepo
  defdelegate get!(queryable, id, opts), to: MemoryRepo

  defdelegate aggregate(queryable, aggregate, opts), to: MemoryRepo
  defdelegate aggregate(queryable, aggregate, field, opts), to: MemoryRepo
end

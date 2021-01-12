defmodule Logflare.RepoWithCache do
  alias Logflare.{Repo, MemoryRepo}

  defdelegate update(changeset), to: Repo
  defdelegate update(changeset, opts), to: Repo
  defdelegate update_all(queryable, updates), to: Repo
  defdelegate update_all(queryable, updates, opts), to: Repo

  defdelegate delete(struct_or_changeset), to: Repo
  defdelegate delete(struct_or_changeset, opts), to: Repo
  defdelegate delete_all(queryable), to: Repo
  defdelegate delete_all(queryable, opts), to: Repo

  defdelegate insert(struct_or_changeset), to: Repo
  defdelegate insert(struct_or_changeset, opts), to: Repo
  defdelegate insert_all(schema_or_source, entries), to: Repo
  defdelegate insert_all(schema_or_source, entries, opts), to: Repo

  defdelegate preload(structs_or_struct_or_nil, preloads), to: MemoryRepo
  defdelegate preload(structs_or_struct_or_nil, preloads, opts), to: MemoryRepo

  defdelegate one(queryable), to: MemoryRepo
  defdelegate one(queryable, opts), to: MemoryRepo
  defdelegate all(queryable), to: MemoryRepo
  defdelegate all(queryable, opts), to: MemoryRepo

  defdelegate get_by(queryable, clauses), to: MemoryRepo
  defdelegate get_by(queryable, clauses, opts), to: MemoryRepo

  defdelegate get(queryable, id), to: MemoryRepo
  defdelegate get(queryable, id, opts), to: MemoryRepo

  defdelegate get!(queryable, id), to: MemoryRepo
  defdelegate get!(queryable, id, opts), to: MemoryRepo
end

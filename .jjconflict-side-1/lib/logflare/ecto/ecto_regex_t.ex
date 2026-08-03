defmodule Ecto.Regex do
  @moduledoc """
  An Ecto type for storing regular expressions in the database. It accepts both string patterns and `Regex` structs.

  ## Usage in Schemas

  ```elixir
  defmodule MyApp.Filter do
    use Ecto.Schema

    schema "filters" do
      field :name, :string
      field :pattern, Ecto.Regex
    end
  end
  ```

  ## Database Migration

  The underlying database type for this Ecto type is `:binary`, which allows for
  efficient storage of compiled regex patterns.

  ```elixir
  defmodule MyApp.Repo.Migrations.CreateFilters do
    def change do
      create table(:filters) do
        add :name, :string, null: false
        add :pattern, :binary, null: false
        timestamps()
      end
    end
  end
  ```
  """

  @behaviour Ecto.Type

  @spec type :: :binary
  def type, do: :binary

  def cast(value) when is_binary(value) do
    case Regex.compile(value) do
      {:ok, r} -> {:ok, r}
      {:error, _} -> :error
    end
  end

  def cast(%Regex{} = value), do: {:ok, value}
  def cast(_), do: :error

  @spec load(any) :: :error | {:ok, any}
  def load(value) do
    {:ok, :erlang.binary_to_term(value)}
  rescue
    e in ArgumentError -> {:error, e}
  end

  def dump(%Regex{} = value) do
    {:ok, :erlang.term_to_binary(value)}
  end

  def dump(_), do: :error

  def embed_as(_) do
    :self
  end

  def equal?(term1, term2) do
    term1 === term2
  end
end

defmodule LogflareWeb.AccessTokensLive.Form do
  @moduledoc false

  use TypedEctoSchema

  import Ecto.Changeset,
    only: [add_error: 3, cast: 3, cast_embed: 3, get_field: 2, validate_required: 2]

  alias LogflareWeb.AccessTokensLive.ResourcePermission

  @primary_key false
  typed_embedded_schema do
    field :description, :string, default: ""
    field :private, :boolean, default: false
    field :admin, :boolean, default: false
    embeds_one :ingest, ResourcePermission, on_replace: :update
    embeds_one :query, ResourcePermission, on_replace: :update
  end

  @spec new() :: t()
  def new do
    %__MODULE__{
      ingest: %ResourcePermission{enabled: true},
      query: %ResourcePermission{}
    }
  end

  @spec change(t(), map()) :: Ecto.Changeset.t()
  def change(form, attrs \\ %{}) do
    form
    |> cast(attrs, [:description, :private, :admin])
    |> validate_required([:private, :admin])
    |> cast_embed(:ingest, required: true, with: &ResourcePermission.changeset/2)
    |> cast_embed(:query, required: true, with: &ResourcePermission.changeset/2)
  end

  @spec validate(t(), map(), [integer()], [integer()]) :: Ecto.Changeset.t()
  def validate(form, attrs, source_ids, endpoint_ids) do
    form
    |> change(attrs)
    |> validate_permissions(source_ids, endpoint_ids)
    |> Map.put(:action, :validate)
  end

  @spec to_scopes(t()) :: [binary()]
  def to_scopes(%__MODULE__{admin: true}), do: ["private:admin"]

  def to_scopes(%__MODULE__{private: true}), do: ["private"]

  def to_scopes(%__MODULE__{} = form) do
    permission_scopes(:ingest, form.ingest) ++ permission_scopes(:query, form.query)
  end

  defp validate_permissions(changeset, source_ids, endpoint_ids) do
    if get_field(changeset, :private) or get_field(changeset, :admin) do
      changeset
    else
      changeset
      |> validate_enabled_permission()
      |> validate_permission(:ingest, "source", source_ids)
      |> validate_permission(:query, "endpoint", endpoint_ids)
    end
  end

  defp validate_enabled_permission(changeset) do
    ingest = get_field(changeset, :ingest)
    query = get_field(changeset, :query)

    if match?(%ResourcePermission{enabled: true}, ingest) or
         match?(%ResourcePermission{enabled: true}, query) do
      changeset
    else
      add_error(changeset, :base, "select at least one scope")
    end
  end

  defp validate_permission(changeset, field, resource, allowed_ids) do
    case get_field(changeset, field) do
      %ResourcePermission{enabled: true, mode: :selected, selected_ids: []} ->
        add_error(changeset, field, "select at least one #{resource}")

      %ResourcePermission{enabled: true, mode: :selected, selected_ids: selected_ids} ->
        if Enum.all?(selected_ids, &(&1 in allowed_ids)),
          do: changeset,
          else: add_error(changeset, field, "contains an invalid selected #{resource}")

      %ResourcePermission{} ->
        changeset

      nil ->
        changeset
    end
  end

  defp permission_scopes(_scope, %ResourcePermission{enabled: false}), do: []

  defp permission_scopes(scope, %ResourcePermission{enabled: true, mode: :all}),
    do: [Atom.to_string(scope)]

  defp permission_scopes(:ingest, %ResourcePermission{
         enabled: true,
         mode: :selected,
         selected_ids: ids
       }),
       do: ids |> Enum.uniq() |> Enum.map(&"ingest:source:#{&1}")

  defp permission_scopes(:query, %ResourcePermission{
         enabled: true,
         mode: :selected,
         selected_ids: ids
       }),
       do: ids |> Enum.uniq() |> Enum.map(&"query:endpoint:#{&1}")
end
